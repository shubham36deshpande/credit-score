SELECT 
                    PSI_CreditMemo2.creditmemo_date,
                    PSI_CreditMemo2.client_id,
                    status.name AS Supplier,
                    status.status_index,
                    status.status,
                    PSI_CreditMemo2.amount ,
                    status.status_short,
                    status.display_name
                FROM  (
                        SELECT 
                            creditmemo_date,
                            client_id,
                            amount 
                        FROM  PSI_CreditMemo2
                        WHERE status_id = 1
                        ) AS PSI_CreditMemo2
                LEFT JOIN (
                        SELECT  
                            crm_classes.quickbooks_id,
                            PSI_Snapshots.client_id,
                            crm_classes.name,
                            psi_client_st_options.status_index,
                            psi_client_st_options.display_name,
                            psi_client_st_options.status_short,
                            psi_client_st_options.status	
                        FROM PSI_Snapshots  
                        LEFT JOIN psi_client_st_options
                        ON psi_client_st_options.status_index =  PSI_Snapshots.contract_states 
                        RIGHT JOIN crm_classes 
                        ON PSI_Snapshots.client_id = crm_classes.quickbooks_id 
                        WHERE psi_client_st_options.type = 1
                         ) AS status
                ON 	PSI_CreditMemo2.client_id = status.quickbooks_id
                where (PSI_CreditMemo2.creditmemo_date >= DATE_SUB('{{ execution_date.strftime('%Y-%m-%d') }}', INTERVAL 729 DAY)) AND (PSI_CreditMemo2.creditmemo_date <= '{{ execution_date.strftime('%Y-%m-%d') }}')
