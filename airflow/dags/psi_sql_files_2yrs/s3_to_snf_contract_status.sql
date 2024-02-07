truncate table PSI_RAW_DATA.PSI.contract_status;
copy into PSI_RAW_DATA.PSI.contract_status(ID,
CLIENT_ID,
NAME,
STATUS_SHORT,
DISPLAY_NAME,
STATUS_INDEX,
STATUS
)
from (select 
  $2 as ID, 
  $3 as CLIENT_ID, 
  $4 as NAME,
  $5 as STATUS_SHORT,
  $6 as DISPLAY_NAME,
  $7 as STATUS_INDEX, 
  $8 as STATUS
FROM @PSI_RAW_DATA.psi.S3_INTEGRATION/psi_data/contract_status.csv)
FILE_FORMAT = (FORMAT_NAME = 'PSI_RAW_DATA.psi.S3_csv_to_snowflake');
