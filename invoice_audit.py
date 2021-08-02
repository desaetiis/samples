"""
ENTRIES FORMATED AS [...] are purposeful obfuscations 
"""
from datetime import date
from datetime import timedelta

import pandas as pd
import prefect
from prefect import Flow
from prefect import Parameter
from prefect import task
from prefect import unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import LocalRun
from prefect.storage import GitHub

from utils.db_utils.db_connector import OnePayConnection
from utils.db_utils.get_cid_list import make_cid_list
from utils.email_utils.email_task import send_email


@task(max_retries=2, retry_delay=timedelta(seconds=10))
def extract_transform(db: str, env: str, ignore_cids: list) -> pd.DataFrame:
	"""
	gets approved invoices in the last 2 weeks
	:param db: str
	:param env: str
	:param ignore_cids: list of str
	:return: pd.DataFrame: object
	"""

	logger = prefect.context.get("logger")
	if db not in ignore_cids:

		sql = """..."""

		logger.info(f"Querying {db}...")

		# CONNECT TO DB AND GET DATA
		database = OnePayConnection(env=env)

		# get connection object for given tenant
		conn = database.get_cid_connection(tenant_id=db)

		# execute query
		try:
			with conn.cursor() as cursor:
				cursor.execute(sql)
				result = cursor.fetchall()
				print(result)
		finally:
			conn.close()

		logger.info(f"sql result {len(result)}")
		if len(result) != 0:
			df = pd.DataFrame(
				data=result,
				columns=['tenant',
						 'id',
						 'issue_date',
						 'vendor_name',
						 'invoice_number',
						 'invoice_number_normalized',
						 'total',
						 # 'unit_id',
						 'agent_id',
						 'approver_id',
						 'approved_date',
						 'status_id']
			)
			df['tenant'] = db
			return df
		else:
			logger.info(f"Empty result for {db}")


@task()
def same_same_same(target_df: pd.DataFrame) -> pd.DataFrame:
	"""runs same_same_same+ same_same_different tests and concatenates the results

    :param target_df: pd.DataFrame
    :return: df: pd.DataFrame
    """

	# SAME SAME SAME TESTS
	logger = prefect.context.get("logger")
	if target_df is not None and len(target_df) != 0:
		# don't allow rows with missing values in the columns below
		target_df.dropna(axis=0, subset=['invoice_number', 'issue_date', 'total', 'vendor_name'],
						 how="any", inplace=True)

		# normalize invoice numbers
		target_df['invoice_number_normalized'] = target_df['invoice_number'].str.replace(r'[\W ]', '')
		# target_df.head()

		# SSSD
		logger.info('SAME: vendor_name, invoice_number, total')
		logger.info('DIFFERENT: `issue_date`')
		target_df2 = target_df[
			target_df.duplicated(['invoice_number_normalized', 'vendor_name', 'total'], keep=False)]
		# logger.info(f"Potential Refund: ${(target_df2['total'].sum() / 2):.2f}")
		target_df2 = target_df2.sort_values(by="invoice_number_normalized")

		logger.info('SAME: vendor_name, invoice_number, issue_date')
		logger.info('DIFFERENT: total')
		target_df1 = target_df[
			target_df.duplicated(['invoice_number_normalized', 'vendor_name', 'issue_date'], keep=False)]
		# logger.info(f"Potential Refund: ${(target_df1['total'].sum() / 2):.2f}")
		target_df1 = target_df1.sort_values(by="invoice_number_normalized")

		logger.info('SAME: invoice_number, issue_date, total')
		logger.info('DIFFERENT: vendor_name')
		target_df4 = target_df[target_df.duplicated(['invoice_number_normalized', 'total', 'issue_date'], keep=False)]
		# logger.info(f"Potential Refund: ${(target_df4['total'].sum() / 2):.2f}")
		target_df4 = target_df4.sort_values(by="invoice_number_normalized")

		logger.info('SAME: vendor_name, issue_date, total')
		logger.info('DIFFERENT: invoice_number')
		target_df3 = target_df[target_df.duplicated(['vendor_name', 'total', 'issue_date'], keep=False)]
		# logger.info(f"Potential Refund: `${(target_df3['total'].sum() / 2):.2f}`")
		target_df3 = target_df3.sort_values(by="total")

		# CONCATENATE ALL TOGETHER
		concatenate_list = [target_df2, target_df1, target_df4]
		target = pd.concat([x for x in concatenate_list if x is not None])

		return target.head(10)  # return first 10 - hardcoded limit


@task()
def combine(dfs: list) -> pd.DataFrame:
	"""
    reduce step to concatenate all results

    :param dfs: list of dataframes
    :return: pd.DataFrame
    """

	logger = prefect.context.get("logger")

	dfs = [x for x in dfs if x is not None]
	if dfs:
		df = pd.concat(dfs).reset_index(drop=True)
		df = df.drop_duplicates(subset=['tenant', 'id'],
								keep="first")
		# df.dropna(axis=0, how='all', inplace=True)
		# print(df)
		logger.info("Combining successful!")
		return df
	else:
		logger.info("Nothing to combine!")


# flow context manager
with Flow("invoice_audit") as f:
	# global variables
	today = date.today()
	yesterday = date.today() - timedelta(days=1)

	# prefect parameters - these can be now accessed and changed in the UI
	environment = Parameter("environment", default="pro")
	email_to = Parameter("email_to", default=["fvadan@onedatasource.com"])

	# other configurations
	IGNORE_CIDS = [...]

	# email config - used to generate different salutation-closing combinations
	email_config = {
		"email_to": email_to,
		"email_subject": f"Invoice Audit {yesterday}",
		"salutation": {
			'g1': ["Hi", "Hey", "Hello", "Greetings"],
			'g2': ["friend", "amigo", ""]  # can add different names here e.g.(Hello Peter)
		},
		"insight": "The invoices below may be duplicates",
		"closing": {
			'c1': ["Have a great", "Enjoy this"],
			'c2': ["day", "week", "month", "year", "century"]
		},
		"robot_name": "Audi Alert",
		"disclaimer": "This is an automated email. This script runs same-same-same and same-same-different test to find duplicates using vendor_name, invoice_number, issue_date, total and unit_id.",
		"formatter": {'header': "{:.0f}"},
		"email_user": False
	}

	# TASK ORCHESTRATION
	# 1. get a list of active tenants to do work on
	cid_list = make_cid_list(env=environment)

	# 2. extract data from db - uses map to auto-spawn as many tasks as needed (one for each tenant in cid_list)
	data = extract_transform.map(db=cid_list,
								 env=unmapped(environment),
								 ignore_cids=unmapped(IGNORE_CIDS))

	# 3. run same_same_same test on each df from no.2 - uses map to spawn as many tasks as needed
	same_same = same_same_same.map(data)

	# 4. combine/reduce all results from previous step into on dtaframe
	combined = combine(same_same)

	# 5. email final result
	email = send_email(data=combined, config=email_config)

if __name__ == '__main__':
	f.storage = GitHub(repo="...",  # name of repo
					   path="...",  # location of flow file in repo
					   ref="master",  # branch
					   # ref="development",  # branch
					   secrets=["GITHUB_ACCESS_TOKEN"],  # name of personal access token secret
					   stored_as_script=True
					   )  # scripts location
	f.run_config = LocalRun()  # where and how a FLOW run should be executed - LocalRun pairs with a LocalAgent
	f.executor = LocalDaskExecutor()  # responsible for running TASKS in a FLOW - LocalDaskExecutor will parallelize task runs across all cores
	f.register(project_name="anomaly_detection", labels=['...'])  # register FLOW with prefect cloud and associate with a specific agent
