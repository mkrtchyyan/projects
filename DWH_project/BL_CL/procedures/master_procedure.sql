CREATE OR REPLACE PROCEDURE bl_cl.master_procedure()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Starting 3NF Loads';
    CALL bl_cl.load_ce_countries();
    CALL bl_cl.load_ce_cities();
    CALL bl_cl.load_ce_customers_scd();         
    CALL bl_cl.load_ce_jobs();
    CALL bl_cl.load_ce_payment_methods();
    CALL bl_cl.load_ce_payment_providers();
    CALL bl_cl.load_ce_payments();
    CALL bl_cl.load_ce_product_groups();
	CALL bl_cl.load_ce_product_categories();
    CALL bl_cl.load_ce_product_types();
    CALL bl_cl.load_ce_products();
    CALL bl_cl.load_ce_staff();
	CALL bl_cl.load_ce_stores();
    CALL bl_cl.load_ce_staff_jobs();
    CALL bl_cl.load_ce_staff_stores();
 	CALL bl_cl.load_ce_sales();

    RAISE NOTICE 'Starting DWH Dimension Loads';

    CALL bl_cl.load_dim_customers_scd();        
    CALL bl_cl.load_dim_dates();
    CALL bl_cl.load_dim_payments();
    CALL bl_cl.load_dim_products();
    CALL bl_cl.load_dim_staff();
    CALL bl_cl.load_dim_stores();
	CALL bl_cl.load_fct_sales_dd();
 

    RAISE NOTICE 'DWH Load Completed';
END;
$$;
