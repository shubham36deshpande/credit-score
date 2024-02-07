truncate table CREDIT_SCORE_PHASE2.PSI.crm_classes;
copy into  CREDIT_SCORE_PHASE2.PSI.crm_classes (
SUPPLIER_ID,
SUPPLIER,
STATUS_INDEX,
STATUS,
DISPLAY_NAME,
STATUS_SHORT
)
from (
select
$2 as SUPPLIER_ID,
$3 as SUPPLIER,
$4 as STATUS_INDEX,
$5 as STATUS,
$6 as DISPLAY_NAME,
$7 as STATUS_SHORT
FROM @CREDIT_SCORE_PHASE2.psi.S3_INTEGRATION/PHASE2/{{ execution_date.strftime('%Y/%m/%d') }}/crm_classes.csv)
FILE_FORMAT = (FORMAT_NAME = 'CREDIT_SCORE_PHASE2.psi.S3_csv_to_snowflake');

