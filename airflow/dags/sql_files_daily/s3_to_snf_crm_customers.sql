truncate table CREDIT_SCORE_PHASE2.PSI.crm_customers;
copy into CREDIT_SCORE_PHASE2.PSI.crm_customers(
NDG_NAME,
NDG_ID,
CUSTOMER_ID,
CUSTOMER,
CUSTOMER_TYPE,
CUSTOMER_TYPE_ID
)
from (
select 
$2 as NDG_NAME,
$3 as NDG_ID,
$4 as CUSTOMER_ID,
$5 as CUSTOMER,
$6 as CUSTOMER_TYPE,
$7 as CUSTOMER_TYPE_ID
FROM @CREDIT_SCORE_PHASE2.psi.S3_INTEGRATION/PHASE2/{{ execution_date.strftime('%Y/%m/%d') }}/crm_customers.csv)
FILE_FORMAT = (FORMAT_NAME = 'CREDIT_SCORE_PHASE2.psi.S3_csv_to_snowflake');