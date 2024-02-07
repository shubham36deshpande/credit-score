# airflow import statements
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

# other import statements
import json
import boto3
import datetime
import numpy as np
import pandas as pd
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from datetime import timedelta, date, datetime
from sklearn.preprocessing import MinMaxScaler

# file import statements
from creditscore import process_snf_data
from clustering_function import clustering_function


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 5),
    'retries': 1,
    'Catchup': True,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': True,
}

# ==========================================
# function for sending email if job fails
# ==========================================


def send_failure_email(context):
	task_instance = context.get("task_instance")
	task_id = task_instance.task_id
	reason = context.get("exception")
	subject = f'STAG - Airflow Task Failure: {task_id}'
	body = f'Task "{task_id}" has failed. Reason: "{reason}"'
	sns_client = boto3.client('sns', region_name='us-east-1')

	topic_arn = 'arn:aws:sns:us-east-1:036619883462:parkstreet-creditscore-dev'

	# Publish a message to the SNS topic.
	response = sns_client.publish(
		TopicArn=topic_arn,
		Message=body,
		Subject=subject,
	)

	if response['ResponseMetadata']['HTTPStatusCode'] == 200:
		print(f"Message sent to SNS topic {topic_arn}")
	else:
		print("Error publishing message to SNS topic")


#################################################################
# DAG starts here
#################################################################

with DAG(
    'creditscore_daily_dag',
    default_args=default_args,
    template_searchpath='/home/ubuntu/airflow/dags/sql_files_daily/',
    schedule_interval='0 3 * * *',
    catchup=False
) as dag:

    # ------------------------------------------------------------
    # TaskGroup to load data from rds to s3.
    # ------------------------------------------------------------
    with TaskGroup('rds_to_s3_group') as rds_to_s3_group:
	    PSI_Invoice_to_s3 = SqlToS3Operator(
            task_id='PSI_Invoice_to_s3',
            query='psi_invoice.sql',
            s3_bucket='parkstreet-credit-score-tables-data',
            s3_key='PHASE2/{{ execution_date.strftime("%Y/%m/%d") }}/PSI_Invoice.csv',
            sql_conn_id='rds_connection',
            aws_conn_id='aws_default',
            replace=True,
            on_failure_callback=send_failure_email
        )
	    chargebacks_to_s3 = SqlToS3Operator(
            task_id='chargebacks_to_s3',
            query='chargebacks.sql',
            s3_bucket='parkstreet-credit-score-tables-data',
            s3_key='PHASE2/{{ execution_date.strftime("%Y/%m/%d") }}/Chargebacks.csv',
            sql_conn_id='rds_connection',
            aws_conn_id='aws_default',
            replace=True,
            on_failure_callback=send_failure_email
        )
	    contract_status_to_s3 = SqlToS3Operator(
            task_id='contract_status_to_s3',
            query='contract_status.sql',
            s3_bucket='parkstreet-credit-score-tables-data',
            s3_key='PHASE2/{{ execution_date.strftime("%Y/%m/%d") }}/contract_status.csv',
            sql_conn_id='rds_connection',
            aws_conn_id='aws_default',
            replace=True,
            on_failure_callback=send_failure_email
        )
	    open_payable_to_s3 = SqlToS3Operator(
            task_id='open_payable_to_s3',
            query='open_payable.sql',
            s3_bucket='parkstreet-credit-score-tables-data',
            s3_key='PHASE2/{{ execution_date.strftime("%Y/%m/%d") }}/open_payable.csv',
            sql_conn_id='rds_connection',
            aws_conn_id='aws_default',
            replace=True,
            on_failure_callback=send_failure_email
       )
	    unapplied_balance_to_s3 = SqlToS3Operator(
            task_id='unapplied_balance_to_s3',
            query='unapplied_balance.sql',
            s3_bucket='parkstreet-credit-score-tables-data',
            s3_key='PHASE2/{{ execution_date.strftime("%Y/%m/%d") }}/unapplied_balance.csv',
            sql_conn_id='rds_connection',
            aws_conn_id='aws_default',
            replace=True,
            on_failure_callback=send_failure_email
        )
	    credit_memo_to_s3 = SqlToS3Operator(
            task_id='credit_memo_to_s3',
            query='credit_memo.sql',
            s3_bucket='parkstreet-credit-score-tables-data',
            s3_key='PHASE2/{{ execution_date.strftime("%Y/%m/%d") }}/credit_memo.csv',
            sql_conn_id='rds_connection',
            aws_conn_id='aws_default',
            replace=True,
            on_failure_callback=send_failure_email
       )
	    other_open_payables_to_s3 = SqlToS3Operator(
            task_id='other_open_payables_to_s3',
            query='other_open_payables.sql',
            s3_bucket='parkstreet-credit-score-tables-data',
            s3_key='PHASE2/{{ execution_date.strftime("%Y/%m/%d") }}/other_open_payables.csv',
            sql_conn_id='rds_connection',
            aws_conn_id='aws_default',
            replace=True
        )
	    cash_balance_to_s3 = SqlToS3Operator(
            task_id='cash_balance_to_s3',
            query='cash_balance.sql',
            s3_bucket='parkstreet-credit-score-tables-data',
            s3_key='PHASE2/{{ execution_date.strftime("%Y/%m/%d") }}/cash_balance.csv',
            sql_conn_id='rds_connection',
            aws_conn_id='aws_default',
            replace=True,
            on_failure_callback=send_failure_email
        )
	    crm_classes_to_s3 = SqlToS3Operator(
            task_id='crm_classes_to_s3',
            query='crm_classes.sql',
            s3_bucket='parkstreet-credit-score-tables-data',
            s3_key='PHASE2/{{ execution_date.strftime("%Y/%m/%d") }}/crm_classes.csv',
            sql_conn_id='rds_connection',
            aws_conn_id='aws_default',
            replace=True,
            on_failure_callback=send_failure_email
        )
	    crm_ndg_to_s3 = SqlToS3Operator(
            task_id='crm_ndg_to_s3',
            query='crm_ndg.sql',
            s3_bucket='parkstreet-credit-score-tables-data',
            s3_key='PHASE2/{{ execution_date.strftime("%Y/%m/%d") }}/crm_ndg.csv',
            sql_conn_id='rds_connection',
            aws_conn_id='aws_default',
            replace=True,
            on_failure_callback=send_failure_email
        )
	    crm_customers_to_s3 = SqlToS3Operator(
            task_id='crm_customers_to_s3',
            query='crm_customers.sql',
            s3_bucket='parkstreet-credit-score-tables-data',
            s3_key='PHASE2/{{ execution_date.strftime("%Y/%m/%d") }}/crm_customers.csv',
            sql_conn_id='rds_connection',
            aws_conn_id='aws_default',
            replace=True,
            on_failure_callback=send_failure_email
        )

	# ---------------------------------------------------
	# TaskGroup to load data from s3 to snowflake.
	# ---------------------------------------------------
    with TaskGroup('s3_to_snf_group') as s3_to_snf_group:
	    s3_to_snf_psi_invoice = SnowflakeOperator(
            task_id='s3_to_snf_psi_invoice',
            sql='s3_to_snf_psi_invoice.sql',
            snowflake_conn_id='snowflake_connection_2',
            on_failure_callback=send_failure_email
        )
	    s3_to_snf_chargebacks = SnowflakeOperator(
            task_id='s3_to_snf_chargebacks',
            sql='s3_to_snf_chargebacks.sql',
            snowflake_conn_id='snowflake_connection_2',
            on_failure_callback=send_failure_email
        )
	    s3_to_snf_credit_memo = SnowflakeOperator(
            task_id='s3_to_snf_credit_memo',
            sql='s3_to_snf_credit_memo.sql',
            snowflake_conn_id='snowflake_connection_2',
            on_failure_callback=send_failure_email
        )
	    s3_to_snf_psi_contract_status = SnowflakeOperator(
            task_id='s3_to_snf_contract_status',
            sql='s3_to_snf_contract_status.sql',
            snowflake_conn_id='snowflake_connection_2',
            on_failure_callback=send_failure_email,
        )
	    s3_to_snf_open_payable = SnowflakeOperator(
            task_id='s3_to_snf_open_payable',
            sql='s3_to_snf_open_payable.sql',
            snowflake_conn_id='snowflake_connection_2',
            on_failure_callback=send_failure_email
        )
	    s3_to_snf_unapplied_balance = SnowflakeOperator(
            task_id = 's3_to_snf_unapplied_balance',
            sql = 's3_to_snf_unapplied_balance.sql',
            snowflake_conn_id = 'snowflake_connection_2',
            on_failure_callback = send_failure_email
        )
	    s3_to_snf_other_open_payables = SnowflakeOperator(
            task_id = 's3_to_snf_other_open_payables',
            sql = 's3_to_snf_other_open_payables.sql',
            snowflake_conn_id = 'snowflake_connection_2',
            on_failure_callback = send_failure_email
        )
	    s3_to_snf_cash_balance = SnowflakeOperator(
            task_id = 's3_to_snf_cash_balance',
            sql = 's3_to_snf_cash_balance.sql',
            snowflake_conn_id = 'snowflake_connection_2',
            on_failure_callback = send_failure_email
        )
	    s3_to_snf_crm_classes = SnowflakeOperator(
            task_id = 's3_to_snf_crm_classes',
            sql = 's3_to_snf_crm_classes.sql',
            snowflake_conn_id = 'snowflake_connection_2',
            on_failure_callback = send_failure_email
	)
	    s3_to_snf_crm_customers = SnowflakeOperator(
            task_id = 's3_to_snf_crm_customers',
            sql = 's3_to_snf_crm_customers.sql',
            snowflake_conn_id = 'snowflake_connection_2',
            on_failure_callback = send_failure_email
        )
	    s3_to_snf_crm_ndg = SnowflakeOperator(
            task_id = 's3_to_snf_crm_ndg',
            sql = 's3_to_snf_crm_ndg.sql',
            snowflake_conn_id = 'snowflake_connection_2',
            on_failure_callback = send_failure_email
        )

    # --------------------------------------------------------------------
	# main function to process the fetched data and create multiple tables
    # --------------------------------------------------------------------
    process_snf_data = PythonOperator(
        task_id = 'process_snf_data',
        python_callable=process_snf_data,
        on_failure_callback = send_failure_email
    )
	
	# ---------------------------------------------------------
	# clustering function to crete all_customer_supplier_score
	# ---------------------------------------------------------
    clustering_function = PythonOperator(
        task_id="clustering_function", 
        python_callable=clustering_function, 
        on_failure_callback=send_failure_email
        )
	
##########################################################################
# Pipeline
rds_to_s3_group >> s3_to_snf_group >> process_snf_data >> clustering_function
##########################################################################
