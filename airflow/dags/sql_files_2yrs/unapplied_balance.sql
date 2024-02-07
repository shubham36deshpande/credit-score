SELECT 
                        supplier.id,
                        status.client_id,
                        status.name AS Supplier,
                        status.status_index,
                        status.status,
                        supplier.unapplied_amount,
                        supplier.invoice_date,
                        status.status_short,
                        status.display_name
                FROM 
                        (
                        SELECT
                                po_num,
                                client_id AS id,
                                ((AVG(total_amount)*100)/75)-AVG(total_amount) AS unapplied_amount,
                                AVG(total_amount) AS total_amount,
                                MAX(txn_date) AS invoice_date
                        FROM supplier_invoice 
                        Group BY po_num
                        ) AS supplier
                LEFT JOIN (
                        SELECT  
                        crm_classes.id,
                        PSI_Snapshots.client_id,
                        crm_classes.name,
                        psi_client_st_options.status_short,
                        psi_client_st_options.display_name,
                        psi_client_st_options.status_index,
                        psi_client_st_options.status	
                        FROM PSI_Snapshots  
                        LEFT JOIN psi_client_st_options
                        ON psi_client_st_options.status_index =  PSI_Snapshots.contract_states 
                        RIGHT JOIN crm_classes 
                        ON PSI_Snapshots.client_id = crm_classes.quickbooks_id 
                        WHERE psi_client_st_options.type = 1
                        ) AS status
                ON supplier.id = status.id
                where (supplier.invoice_date >= DATE_SUB('{{ execution_date.strftime('%Y-%m-%d') }}', INTERVAL 729 DAY)) AND (supplier.invoice_date <= '{{ execution_date.strftime('%Y-%m-%d') }}')
