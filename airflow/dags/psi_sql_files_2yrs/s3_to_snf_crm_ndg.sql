truncate table PSI_RAW_DATA.PSI.crm_ndg;
copy into  PSI_RAW_DATA.PSI.crm_ndg (
id,
name,
works_with_bdn,
credentials,
online_access_sop,
bdn_authorization_letter_sop,
online_access_possible,
distributor_data,
online_system_url,
online_system_notes,
type
)
from (
select
$2 as id,
$3 as name,
$4 as works_with_bdn,
$5 as credentials,
$6 as online_access_sop,
$7 as bdn_authorization_letter_sop,
$8 as online_access_possible,
$9 as distributor_data,
$10 as online_system_url,
$11 as online_system_notes,
$12 as type
FROM @PSI_RAW_DATA.psi.S3_INTEGRATION/psi_data/crm_ndg.csv)
FILE_FORMAT = (FORMAT_NAME = 'PSI_RAW_DATA.psi.S3_csv_to_snowflake');
