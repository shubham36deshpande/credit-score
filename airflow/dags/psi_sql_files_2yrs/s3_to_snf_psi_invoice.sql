copy into PSI_RAW_DATA.PSI.psi_invoice(
SUPPLIER_ID,
CLIENT_NAME,
INVOICE_DATE,
INVOICE_NUM,
PO_NUMBER,
QUICKBOOKS_ID,
NDG_ID,
NDG_NAME,
CUSTOMER_ID,
CUSTOMER_NAME,
CUSTOMER_TYPE_ID,
CUSTOMER_TYPE,
ISPAID,
BALANCE,
INVOICE_AMOUNT,
TXN_AMOUNT,
TXN_DATE,
MEMO,
TXN_TYPE,
DUEDATE,
SHIP_LOCATION
)
from
(
select
$2 as SUPPLIER_ID,
$3 as CLIENT_NAME,
$4 as INVOICE_DATE,
$5 as INVOICE_NUM,
$6 as PO_NUMBER,
$7 as QUICKBOOKS_ID,
$8 as NDG_ID,
$9 as NDG_NAME,
$10 as CUSTOMER_ID,
$11 as CUSTOMER_NAME,
$12 as CUSTOMER_TYPE_ID,
$13 as CUSTOMER_TYPE,
$14 as ISPAID,
$15 as BALANCE,
$16 as INVOICE_AMOUNT,
$17 as TXN_AMOUNT,
$18 as TXN_DATE,
$19 as MEMO,
$20 as TXN_TYPE,
$21 as DUEDATE,
$22 as SHIP_LOCATION
FROM @PSI_RAW_DATA.PSI.S3_INTEGRATION/psi_data/PSI_Invoice.csv)
FILE_FORMAT =(FORMAT_NAME ='PSI_RAW_DATA.psi.S3_csv_to_snowflake');
