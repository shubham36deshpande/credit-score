SELECT 
	crm_classes.id AS supplier_id,
    crm_classes.name AS client_name,
    PSI_Invoices.invoice_date, 
    PSI_Invoices.invoice_num,
    PSI_Invoices.po_number,
    crm_customers.quickbooks_id,
    crm_customers.ndg_id,
    crm_customers.ndg_name,
    crm_customers.id AS customer_id,
    crm_customers.name AS customer_name,
    crm_customer_types.id AS customer_type_id,
    crm_customers.customer_type, 
    PSI_Invoices.IsPaid,
    PSI_Invoices.balance,
    PSI_Invoices.amount AS invoice_amount,
    PSI_Invoice_Link_Payment_Txn.amount AS txn_amount,
    IF(((PSI_Invoice_Link_Payment_Txn.memo LIKE '%Ear%pay%a%') or (PSI_Invoice_Link_Payment_Txn.memo LIKE '%factor%a%')),
        (SELECT MIN(t2.txn_date)
         FROM PSI_Invoice_Link_Payment_Txn t2
         WHERE t2.txn_id = PSI_Invoice_Link_Payment_Txn.txn_id
         AND t2.txn_date > PSI_Invoice_Link_Payment_Txn.txn_date),
        PSI_Invoice_Link_Payment_Txn.txn_date) AS txn_date,
    PSI_Invoice_Link_Payment_Txn.memo,
    PSI_Invoice_Link_Payment_Txn.txn_type,
    PSI_Invoices.duedate,
    PSI_Invoices.ship_location
FROM PSI_Invoices
LEFT JOIN crm_classes 
    ON PSI_Invoices.client_id = crm_classes.quickbooks_id 
LEFT JOIN (
			SELECT   
					crm_customers.id,
					crm_customers.quickbooks_id, 
					crm_ndg.id AS ndg_id,
					crm_ndg.ndg_name,
			   		crm_customers.name,
			    	crm_customers.customer_type
			FROM crm_customers 
			LEFT JOIN 
					(
					SELECT id,
						   name AS ndg_name
					FROM crm_ndg 
				    ) crm_ndg
			ON crm_customers.ndg_id = crm_ndg.id 
		  ) crm_customers
    ON PSI_Invoices.Location_Job_Id = crm_customers.quickbooks_id 
LEFT JOIN PSI_Invoice_Link_Payment_Txn
    ON PSI_Invoice_Link_Payment_Txn.txn_id = PSI_Invoices.txn_id 
LEFT JOIN crm_customer_types
    ON crm_customer_types.`type`  = crm_customers.customer_type
WHERE (crm_customers.quickbooks_id IS NOT NULL) 
    AND (crm_classes.quickbooks_id  IS NOT NULL) 
    AND (PSI_Invoices.client_id != '') 
    AND (PSI_Invoices.Location_Job_Id != '')
    AND (PSI_Invoices.amount !=0)
    AND (PSI_Invoices.invoice_date >= DATE_SUB('{{ execution_date.strftime('%Y-%m-%d') }}', INTERVAL 0 DAY)) AND (PSI_Invoices.invoice_date <= '{{ execution_date.strftime('%Y-%m-%d') }}')
