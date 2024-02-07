SELECT 
            `bm`.`transaction_number` AS `reference_num`, 
            cls.id AS supplier_id,
            `cls`.`name` AS `client_name`, 
            `entity`.`name` AS `entity`, 
            `bm`.`transaction_type` AS `type`, 
            GROUP_CONCAT(DISTINCT substring_index(acct.account_full_name, ':', 1)) summary_acct, 
            IF(bm.is_paid=1,'Paid','Open') bill_status, 
            crm_ndg.id AS ndg_id,
            crm_ndg.name AS ndg_name,
            cc.id AS customer_id,
            crm_customer_types.id AS customer_type_id,
            cc.customer_type,
            `comp`.`company_name` AS `vendor`,
            cc.name AS customer_name,
            (SUM(bmli.amount)) AS total_amount, 
            `bm`.`balance` AS `open_balance`, 
            COALESCE(SUM(bmp2.bm_accounting_payment_amount), 0) AS paid_amount, 
            `bm`.`transaction_date` AS `bill_date`, 
            `bm`.`due_date` AS `cb_duedate`,
            max(bmp2.bm_accounting_payment_date) AS paid_date
        FROM 
        `bm_transactions` AS `bm` 
        INNER JOIN `bm_transaction_lines` AS `bmli` 
        ON `bmli`.`id` = ( SELECT MIN(id) FROM bm_transaction_lines 
                           line WHERE line.bm_transaction_id = bm.id 
                           GROUP BY bm_transaction_id 
                         ) 
        INNER JOIN `crm_companies_vendors` as `comp_vendor` 
        ON `comp_vendor`.`vendor_id` = `bm`.`vendor_id` 
        LEFT JOIN `im_metadata` as `im` 
        ON `im`.`txn_id` = `bm`.`accounting_transaction_id` 
        INNER JOIN `crm_companies` as `comp` 
        ON `comp`.`id` = `comp_vendor`.`company_id` 
        LEFT JOIN `crm_classes` as `cls` 
        ON `cls`.`id` = `bmli`.`client_class_id` 
        LEFT JOIN `subsidiaries` as `entity` 
        ON `entity`.`id` = `bm`.`subsidiary_id` 
        LEFT JOIN `PSI_Accounts` as `acct` 
        ON `acct`.`account_id` = `bmli`.`expense_account_id` 
        LEFT JOIN `psi_products_brands` as `brand` 
        ON `brand`.`id` = `bmli`.`brand_id` 
        LEFT JOIN `PSI_Markets` as `market` 
        ON `market`.`id` = `bmli`.`market_id` 
        LEFT JOIN `bm_payments` as `bmp2` 
        ON `bmp2`.`bm_transaction_id` = `bm`.`id` 
        LEFT JOIN crm_companies_customers
        ON crm_companies_customers.company_id = `comp`.`id`
        LEFT JOIN crm_customers AS cc
        ON cc.id  = crm_companies_customers.customer_id
        LEFT JOIN crm_customer_types
        ON crm_customer_types.`type`  = cc.customer_type
        LEFT JOIN crm_ndg 
        ON cc.ndg_id = crm_ndg.id
        WHERE `bm`.`archived` = 0 and (cc.id is not null) and (`bm`.`transaction_date`>= DATE_SUB('{{ execution_date.strftime('%Y-%m-%d') }}', INTERVAL 0 DAY)) AND (`bm`.`transaction_date` <= '{{ execution_date.strftime('%Y-%m-%d') }}')
        GROUP BY `bm`.`transaction_date`, `bm`.`id` 
        HAVING summary_acct = 'Chargebacks'
