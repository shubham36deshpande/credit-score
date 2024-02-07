SELECT  
            crm_classes.id as supplier_id,
            crm_classes.name as Supplier,
            psi_client_st_options.status_index,
            psi_client_st_options.status,
            psi_client_st_options.display_name,
            psi_client_st_options.status_short
            FROM crm_classes 
            LEFT JOIN PSI_Snapshots 
            ON PSI_Snapshots.client_id = crm_classes.quickbooks_id 
            LEFT JOIN psi_client_st_options
            ON psi_client_st_options.status_index =  PSI_Snapshots.contract_states AND psi_client_st_options.type = 1
