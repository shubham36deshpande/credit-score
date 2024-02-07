SELECT 
	   crm_ndg.ndg_name AS ndg_name,
       cc.ndg_id AS ndg_id,
	   cc.id as customer_id,
	   cc.name as Customer,
       cc.customer_type,
       crm_customer_types.id AS customer_type_id
from crm_customers cc
LEFT JOIN crm_customer_types
ON crm_customer_types.`type`  = cc.customer_type
LEFT JOIN 
		(
		SELECT id,
			   name AS ndg_name
		FROM crm_ndg 
	    ) crm_ndg
ON cc.ndg_id = crm_ndg.id
