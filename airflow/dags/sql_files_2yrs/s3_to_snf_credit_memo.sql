copy into CREDIT_SCORE_PHASE2.PSI.credit_memo(
CREDITMEMO_DATE,
CLIENT_ID,
SUPPLIER,
STATUS_INDEX,
STATUS,
AMOUNT,
STATUS_SHORT,
DISPLAY_NAME
)
from (
select
$2 as CREDITMEMO_DATE,
$3 as CLIENT_ID,
$4 as SUPPLIER,
$5 as STATUS_INDEX,
$6 as STATUS,
$7 as AMOUNT,
$8 as STATUS_SHORT,
$9 as DISPLAY_NAME  
FROM @CREDIT_SCORE_PHASE2.psi.S3_INTEGRATION/demo/credit_memo.csv)
FILE_FORMAT = (FORMAT_NAME = 'CREDIT_SCORE_PHASE2.psi.S3_csv_to_snowflake');
