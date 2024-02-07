truncate table PSI_RAW_DATA.PSI.cash_balance;
copy into PSI_RAW_DATA.PSI.cash_balance(ID,
CLIENT_ID,
Supplier,
STATUS_INDEX,
STATUS,
display_name,
status_short,
CASH_BALANCE)
from (select
$2 as ID,
$3 as CLIENT_ID,
$4 as Supplier,
$5 as STATUS_INDEX,
$6 as STATUS,
$7 as DISPLAY_NAME,
$8 as STATUS_SHORT,
$9 as CASH_BALANCE
FROM @PSI_RAW_DATA.psi.S3_INTEGRATION/psi_data/cash_balance.csv)
FILE_FORMAT = (FORMAT_NAME = 'PSI_RAW_DATA.PSI.S3_csv_to_snowflake');
