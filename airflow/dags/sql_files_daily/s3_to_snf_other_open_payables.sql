copy into CREDIT_SCORE_PHASE2.PSI.other_open_payables(
reference_num,
replace_ref_num,
new_total,
client_id,
Supplier,
type,
summary_acct,
bill_status,
vendor,
total_amount,
other_open_payables,
paid_amount,
bill_date,
cb_duedate,
paid_date,
id,
cl_id,
name,
status_index,
display_name,
status_short,
status
)
from (select $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23 as OTHER_OPEN_PAYABLES
FROM @CREDIT_SCORE_PHASE2.PSI.S3_INTEGRATION/PHASE2/{{ execution_date.strftime('%Y/%m/%d') }}/other_open_payables.csv)
FILE_FORMAT = (FORMAT_NAME = 'CREDIT_SCORE_PHASE2.psi.S3_csv_to_snowflake');
