truncate table PSI_RAW_DATA.PSI.crm_customers;
copy into PSI_RAW_DATA.PSI.crm_customers(
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
FROM @PSI_RAW_DATA.PSI.S3_INTEGRATION/psi_data/crm_customers.csv)
FILE_FORMAT = (FORMAT_NAME = 'PSI_RAW_DATA.psi.S3_csv_to_snowflake');
