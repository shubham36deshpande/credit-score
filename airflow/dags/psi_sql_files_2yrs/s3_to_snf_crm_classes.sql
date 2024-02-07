truncate table PSI_RAW_DATA.PSI.crm_classes;
copy into  PSI_RAW_DATA.PSI.crm_classes (
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
FROM @PSI_RAW_DATA.psi.S3_INTEGRATION/psi_data/crm_classes.csv)
FILE_FORMAT = (FORMAT_NAME = 'PSI_RAW_DATA.psi.S3_csv_to_snowflake');
