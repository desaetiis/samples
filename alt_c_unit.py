from datetime import date
from datetime import datetime
from datetime import timedelta

import prefect
import regex as re
from prefect import Flow
from prefect import Parameter
from prefect import task
from prefect import unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import LocalRun
from prefect.storage import GitHub

from utils.db_utils.db_connector import OnePayConnection
from utils.db_utils.get_cid_list import make_cid_list


# FUNCTIONS
def address_validator(address: str) -> str:
    """
    validates street address to common us formats see https://pe.usps.com/cpim/ftp/pubs/Pub28/pub28.pdf
    :param address: str
    :return: address: str
    """

    is_valid = re.compile(r"^\d+|(?i)^p\.?\s*o\.?\s*")

    if address and re.match(is_valid, address):
        return address


def preprocess_address(address: str) -> str:
    """
    Initial street address cleaning
    :param address: str
    :return: address: str
    """

    if (address is not None) and (address != ""):
        address = address.lower()

        # regex patterns for elimination ### ORDER MATTERS ###
        extra_spaces = r"\s+"

        # street type
        addr_type = r"(,?[ ](street|str?|avenue|ave?|road|rd|lane|\bln\b|place|\bpl\b|boulevard|blvd|drive|dr|circle|cir))$"
        leading_spaces = r"^\s+"
        trailing_spaces = r"\s+$"
        north, south, east, west = (
            r"\bn\.?\b|\bn\b|north\b", r"\bs\.?\b|\bs\b|south\b", r"\be\.?\b|\be\b|east\b", r"\bw\.?\b|\bw\b|west\b")
        po = r"\bp\.?\s?o\.?\s?\b"
        suite = r",?\s(suite|ste\b|unit|apartment|\bapt|building|bldg|floor|room).*$"  # WARNING "ste" interferes with the french addresses

        # Process addresses
        address = address.lower()
        address = re.sub(r"[,]", " ", address)  # Eliminate commas
        address = re.sub(extra_spaces, " ", address)  # Eliminate duplicate whitespaces
        address = re.sub(leading_spaces, "", address)  # Eliminate leading whitespaces
        address = re.sub(trailing_spaces, "", address)  # Eliminate trailing whitespaces
        address = re.sub(suite, "", address)
        address = re.sub(r"\.$", "", address)  # Eliminate trailing dots
        address = re.sub(addr_type, "", address)  # Eliminate [inc, ltd], etc at the end of the name
        address = re.sub(po, "", address)  # Eliminate "po" from "po box 23558"

        # Account for all spelling possibilities
        address = re.sub(north, "n.(?:orth)?", address)
        address = re.sub(south, "s.(?:outh)?", address)
        address = re.sub(east, "e.(?:ast)?", address)
        address = re.sub(west, "w.(?:est)?", address)

        # Address Standardization — County, State, Local Highways transformations (PG79)
        address = re.sub(r"\broad\b|\brd\b", "r(?:oa)?d", address)
        address = re.sub(r"highway|\bhwy\b", "h(?:ig)?h?wa?y", address)
        address = re.sub(r"parkway|pkwy|\bpwy\b", "p(?:ar)?k?wa?y", address)
        address = re.sub(r"freeway|\bfwy\b", "f(?:ree)?wa?y", address)
        address = re.sub(r"route|\brte\b|\brt\b", "r(?:ou)?te?", address)
        address = re.sub(r"county|\bcnty\b", "c(?:ou)?nty", address)
        address = re.sub(r"street|\bst\b|\bstr\b", "str?(?:eet)?",
                         address)  # just "st" is not used - interferes with "west" and "east"
        address = re.sub(r"\bcourt\b|\bct\b", "c(?:our)?t", address)
        address = re.sub(r"place|\bpl\b", "pl(?:ace)?", address)
        address = re.sub(r"state", "st(?:ate)?", address)
        address = re.sub(r"bypass|\bbyp\b", "byp(?:ass)?", address)

        if len(address) < 4:
            address = f"{address}\\\\b"

        return address


def address_regex_transformations(address: str) -> str:
    """
    Escaping regex keywords to work with SQL
    :param address: str
    :return: address: str
    """
    if (address is not None) and (address != ""):
        address = re.sub("[ ]", "\\\\\\s*", address)  # Replace single spaces with \\s* to escape for SQL
        address = re.sub("[.]", "\\\\\\.?", address)  # Replace periods with \\.? to escape for SQL
        address = re.sub("[-]", "\\\\\\-", address)  # Replace '-' with \\- to escape for SQL
        address = re.sub("[/]", "\\\\\\/", address)  # Replace '-' with \\- to escape for SQL
        return address


def build_address_regex(address: str) -> str:
    """
    Takes a string and transforms it onto a regular expression to be inserted into alt_table
    :param address: str
    :return: alt_name_expr: str
    """

    if (address is not None) and (address != ""):  # no po box - too many dupes
        alt_name_expr = address_regex_transformations(address)
        # For vendor names with less than 6 characters add word delimiters
        # to avoid matching inside a random word ('agi' in fragile)
        if len(alt_name_expr) < 6:
            alt_name_expr = "\\\\b" + alt_name_expr + "\\\\b"  # Build final regex
        else:
            alt_name_expr = alt_name_expr  # Build final regex
        return alt_name_expr


def build_address_regex_nobox(address: str, vendor_city: str, ignore_list: list) -> str:
    """
    Takes a string and transforms it onto a regular expression to be inserted into regex table.
    Different method for dealing with PO BOX like addresses.
    :param address: str
    :param vendor_city:
    :param ignore_list:
    :return:
    """

    if (address is not None) and (address != "") and (address not in ignore_list):
        if "box" not in address:
            alt_name_expr = address_regex_transformations(address)
            return alt_name_expr
        else:
            if (vendor_city is not None) and (vendor_city != ""):
                alt_name_expr = address_regex_transformations(address)
                alt_name_expr = alt_name_expr + "\\\\b.*?" + address_regex_transformations(vendor_city.lower())
                return alt_name_expr


@task(max_retries=2, retry_delay=timedelta(seconds=10), tags=["database", "mysql"])
def get_new_units(db: str,
                  env: str,
                  ignore_cids: list,
                  window: int = 1,
                  window_unit: str = 'DAY'):
    """
    gets the new units added YESTERDAY per cid
    :param db: str
    :param env: str
    :param ignore_cids: list
    :param window: int
    :param window_unit: str
    :return: N/A
    """

    logger = prefect.context.get("logger")
    if db not in ignore_cids:

        sql = """..."""

        logger.info(f"Querying {db}...")
        # CONNECT TO DB AND GET DATA
        database = OnePayConnection(env=env)
        logger.info(f"Querying {db} {env.upper()}...")
        # GET CONNECTION OBJECT FOR GIVEN TENANT
        conn = database.get_cid_connection(tenant_id=db)

        # execute query
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchall()
                print(result)
        finally:
            conn.close()

        logger.info(f"query result {len(result)}")
        if result:
            for row in result:
                print(f'- Found a new unit in {db}')
                logger.info(f'- Found a new unit in {db}')

                # PARSE RESULT INTO vendor_id AND vendor_name
                store_id, str_address = parse_address(row)

                # CLEAN ADDRESSES
                c_unit_alt_name_expr = build_address_regex(preprocess_address(str_address))

                # INSERT NEWLY BUILT REGEXES INTO DB
                sql_insert_st = f'...'

                logger.info(sql_insert_st)

                # CONNECT TO DB MAKE INSERTS
                database2 = OnePayConnection(env=env)
                # get connection object for given tenant
                conn2 = database2.get_cid_connection(tenant_id=db)
                logger.info(f"Working on {db} {env.upper()}...")

                # EXECUTE QUERY
                try:
                    with conn2.cursor() as cursor2:
                        cursor2.execute(sql_insert_st)
                    conn2.commit()
                    logger.info(f"Insert successful.\n")
                except Exception as e:
                    print(e)
                    conn2.rollback()
                    logger.info("Transaction rolled back.\n")

                finally:
                    conn2.close()

        else:
            print(f"- No new stores for {db}")


def parse_address(row):
    """
    Filter result down to vendor id and vendor name
    :param row: mysql result object record - 1 row
    :return: tuple
    """

    unit_id = row['unit_id']
    address = row['address']
    return unit_id, address


# PREFECT FLOW CONTEXT MANAGER
with Flow("unit_address_regex") as f:
    # GLOBALS
    yesterday = date.today() - timedelta(days=1)
    today = prefect.context.get("today")
    now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

    # PARAMETERS
    email_to = Parameter("email_to", default=["fvadan@onedatasource.com"])
    environment = Parameter("environment", default="pro")

    # other configurations
    IGNORE_CIDS = ['...']

    # TASK ORCHESTRATION
    # 1. GET LIST OF ACTIVE TENANTS
    cid_list = make_cid_list(env=environment)

    # 2.GET NEW STORES, BUILD NEW REGEXES AND INSERT INTO DB TODO: BREAK APART TASKS
    get_stores = get_new_units.map(db=cid_list,
                                   ignore_cids=unmapped(IGNORE_CIDS),
                                   env=unmapped(environment))

if __name__ == '__main__':
    # PREFECT CONFIGS
    f.storage = GitHub(repo="...",  # name of repo
                       path="...",  # location of flow file in repo
                       ref="master",  # branch
                       # ref="development",  # branch
                       secrets=["GITHUB_ACCESS_TOKEN"]  # name of personal access token secret
                       )
    f.run_config = LocalRun()
    f.executor = LocalDaskExecutor()

    f.register(project_name="alt_tables", labels=['...'])
