copy into PSI_RAW_DATA.PSI.open_payable(ID,
CLIENT_ID,
NAME,
STATUS_INDEX,
STATUS,
OPEN_PAYABLE,
INVOICE_DATE,
DUE_DATE,
STATUS_SHORT,
DISPLAY_NAME)
from (select $2 as ID, 
             $3 as CLIENT_ID, 
             $4 as STATUS_ID,
             $5 as NAME, 
             $6 as STATUS, 
             $7 as OPEN_PAYABLE,
             $8 as INOVICE_DATE, 
             $9 as DUE_DATE,
             $10 as STATUS_SHORT,
             $11 as DISPLAY_NAME
FROM @PSI_RAW_DATA.psi.S3_INTEGRATION/psi_data/open_payable.csv)
FILE_FORMAT = (FORMAT_NAME = 'PSI_RAW_DATA.psi.S3_csv_to_snowflake');
