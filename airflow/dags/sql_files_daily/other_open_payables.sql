SELECT * from
(
SELECT 
            `bm`.`transaction_number` AS `reference_num`, 
             CASE 
            WHEN `bm`.`transaction_type` = 'credit' THEN REPLACE(`bm`.`transaction_number`, 'CR', 'A')
           ELSE 0
           END AS replace_ref_num,
           
            IF(COALESCE(SUM(bmp2.bm_accounting_payment_amount), 0)=0, IF(`bm`.`balance`>(SUM(bmli.amount)),`bm`.`balance`,SUM(bmli.amount)),COALESCE(SUM(bmp2.bm_accounting_payment_amount), 0)+(`bm`.`balance`)) as new_total,
                  
            cls.id AS client_id,
            `cls`.`name` AS `Supplier`, 
            `bm`.`transaction_type` AS `type`,
            
            GROUP_CONCAT(DISTINCT substring_index(acct.account_full_name, ':', 1)) summary_acct, 
            IF(bm.is_paid=1,'Paid','Open') bill_status, 
            `comp`.`company_name` AS `vendor`,
            (SUM(bmli.amount)) AS total_amount, 
            `bm`.`balance` AS `other_open_payables`, 
            COALESCE(SUM(bmp2.bm_accounting_payment_amount), 0) AS paid_amount, 
            `bm`.`transaction_date` AS `bill_date`, 
            `bm`.`due_date` AS `cb_duedate`,
            max(bmp2.bm_accounting_payment_date) AS paid_date
      
           
        FROM 
        `bm_transactions` AS `bm` 
        LEFT JOIN `bm_transaction_lines` AS `bmli` 
        ON `bmli`.`id` = ( SELECT MIN(id) FROM bm_transaction_lines 
                           line WHERE line.bm_transaction_id = bm.id 
                           GROUP BY bm_transaction_id 
                         ) 
        left JOIN `crm_companies_vendors` as `comp_vendor` 
        ON `comp_vendor`.`vendor_id` = `bm`.`vendor_id` 
        LEFT JOIN `crm_companies` as `comp` 
        ON `comp`.`id` = `comp_vendor`.`company_id` 
        LEFT JOIN `crm_classes` as `cls` 
        ON `cls`.`id` = `bmli`.`client_class_id` 
        LEFT JOIN `PSI_Accounts` as `acct` 
        ON `acct`.`account_id` = `bmli`.`expense_account_id` 
        LEFT JOIN `bm_payments` as `bmp2` 
        ON `bmp2`.`bm_transaction_id` = `bm`.`id` 
        WHERE `bm`.`archived` = 0 
        and (`bm`.`transaction_date`>= DATE_SUB('{{ execution_date.strftime('%Y-%m-%d') }}', INTERVAL 0 DAY)) AND (`bm`.`transaction_date` <= '{{ execution_date.strftime('%Y-%m-%d') }}')
        GROUP BY `bm`.`transaction_date`, `bm`.`id` 
        )abc
        LEFT JOIN (
                    SELECT  
                    crm_classes.id,
                    PSI_Snapshots.client_id as cl_id,
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
        ON abc.client_id = status.id
        where abc.bill_status = 'Open' and abc.client_id is not null
