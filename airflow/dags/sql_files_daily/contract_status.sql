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
            ON PSI_Snapshots.client_id = crm_classes.quickbooks_id and psi_client_st_options.type = 1
