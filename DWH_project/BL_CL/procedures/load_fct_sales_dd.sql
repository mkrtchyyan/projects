--LOAD FACt TABLE
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_FCT_SALES_DD()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_inserted INT := 0;
    archive_cutoff_dt INT := TO_CHAR((CURRENT_DATE - INTERVAL '3 months')::DATE, 'YYYYMM01')::INT;
    current_month_start DATE := DATE_TRUNC('month', CURRENT_DATE);
    next_month_start DATE := current_month_start + INTERVAL '1 month';
    month_start DATE;
    part_name TEXT;
    part_start INT;
    part_end INT;
BEGIN
    -- creating archive partition for data older than 3 months
    BEGIN EXECUTE format('CREATE TABLE IF NOT EXISTS bl_dm.fct_sales_dd_archive  PARTITION OF bl_dm.fct_sales_dd   FOR VALUES FROM (MINVALUE) TO (%L)', archive_cutoff_dt);
        -- Update constraint to ensure only old data goes here
        EXECUTE format('ALTER TABLE bl_dm.fct_sales_dd_archive  DROP CONSTRAINT IF EXISTS fct_sales_dd_archive_event_dt_check');
        EXECUTE format('ALTER TABLE bl_dm.fct_sales_dd_archive ADD CONSTRAINT fct_sales_dd_archive_event_dt_check CHECK ("EVENT_DT" < %L)', archive_cutoff_dt);
    EXCEPTION WHEN OTHERS THEN  CALL BL_CL.LOG_ETL('LOAD_FCT_SALES_DD', 0, 'Archive partition error: ' || SQLERRM);
    END;
    -- creating monthly partitions for rolling window(3 months back to 1 month forward)
    month_start := DATE_TRUNC('month',CURRENT_DATE -INTERVAL '3 months');
    WHILE month_start <= next_month_start 
    LOOP
        part_name := 'fct_sales_dd_' || TO_CHAR(month_start, 'YYYYMM');
        part_start := TO_CHAR(month_start, 'YYYYMMDD')::INT;
        part_end := TO_CHAR(month_start + INTERVAL '1 month', 'YYYYMMDD')::INT;
        BEGIN
            -- only creating partition if it doesn't exist
            IF NOT EXISTS (  SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = 'bl_dm' AND c.relname = part_name) THEN
                
            EXECUTE format('CREATE TABLE if not exists bl_dm.%I PARTITION OF bl_dm.fct_sales_dd   FOR VALUES FROM (%L) TO (%L)',  part_name, part_start, part_end);
            END IF;
        EXCEPTION WHEN OTHERS THEN  CALL BL_CL.LOG_ETL('LOAD_FCT_SALES_DD', 0, 'Partition ' || part_name || ' error: ' || SQLERRM);
        END;
        month_start := month_start + INTERVAL '1 month';
    END LOOP;
    WITH prepared_data AS (
        SELECT  NEXTVAL('BL_DM.FCT_SALES_DD_SEQ') AS "SALE_SURR_ID",
            COALESCE(s."SALE_SRC_ID", 'n.a') AS "SALE_ID",
            COALESCE(s."SALE_ID", -1) AS "SALE_SOURCE_ID",
            COALESCE(c."CUSTOMER_SURR_ID", -1) AS "CUSTOMER_ID",
            COALESCE(st."STAFF_SURR_ID", -1) AS "STAFF_ID",
            COALESCE(store."STORE_SURR_ID", -1) AS "STORE_ID",
            COALESCE(pay."PAYMENT_SURR_ID", -1) AS "PAYMENT_ID",
            COALESCE(mgr."STAFF_SURR_ID", -1) AS "MANAGER_ID",
            COALESCE(p."PRODUCT_SURR_ID", -1) AS "PRODUCT_ID",
            coalesce(s."EVENT_DT",-1) as "EVENT_DT",
            s."QUANTITY",
            s."LINE_ITEM_AMOUNT",
            s."COST",
            s."PROFIT",
            s."PROFIT_MARGIN",
            s."DELIVERY_FEE",
            CURRENT_TIMESTAMP AS "TA_INSERT_DT",
            CURRENT_TIMESTAMP AS "TA_UPDATE_DT",
            'BL_3NF' AS "SOURCE_SYSTEM",
            'CE_SALES' AS "SOURCE_ENTITY"
        FROM BL_3NF.CE_SALES s
        LEFT JOIN BL_DM.DIM_CUSTOMERS_SCD c ON coalesce(s."CUSTOMER_ID",-1) =c."CUSTOMER_SOURCE_ID"
        LEFT JOIN BL_DM.DIM_STAFF st ON coalesce(s."STAFF_ID",-1) = st."STAFF_SOURCE_ID"
        LEFT JOIN BL_DM.DIM_STORES store ON coalesce(s."STORE_ID",-1) = store."STORE_SOURCE_ID"
        LEFT JOIN BL_DM.DIM_STAFF mgr ON coalesce(s."MANAGER_ID",-1) =mgr."STAFF_SOURCE_ID"
        LEFT JOIN BL_DM.DIM_PRODUCTS p ON coalesce(s."PRODUCT_ID",-1) =p."PRODUCT_SOURCE_ID"
        LEFT JOIN BL_DM.DIM_PAYMENTS pay ON coalesce(s."PAYMENT_ID",-1) = pay."PAYMENT_SOURCE_ID")
    INSERT INTO bl_dm.fct_sales_dd("SALE_SURR_ID",
									"SALE_ID" ,
									"SALE_SOURCE_ID" ,
									"CUSTOMER_ID",
									"STAFF_ID",
									"STORE_ID",
									"PAYMENT_ID",
									"MANAGER_ID",
									"PRODUCT_ID",
									"EVENT_DT",
									"QUANTITY",
									"LINE_ITEM_AMOUNT" ,
									"COST" ,
									"PROFIT" ,
									"PROFIT_MARGIN" ,
									"DELIVERY_FEE" ,
									"TA_INSERT_DT",
									"TA_UPDATE_DT",
									"SOURCE_SYSTEM" ,
									"SOURCE_ENTITY" )
    SELECT "SALE_SURR_ID",
			"SALE_ID" ,
			"SALE_SOURCE_ID" ,
			"CUSTOMER_ID",
			"STAFF_ID",
			"STORE_ID",
			"PAYMENT_ID",
			"MANAGER_ID",
			"PRODUCT_ID",
			"EVENT_DT",
			"QUANTITY",
			"LINE_ITEM_AMOUNT",
			"COST",
			"PROFIT" ,
			"PROFIT_MARGIN" ,
			"DELIVERY_FEE" ,
			"TA_INSERT_DT",
			"TA_UPDATE_DT",
			"SOURCE_SYSTEM" ,
			"SOURCE_ENTITY" 
FROM prepared_data
    ON CONFLICT ("EVENT_DT", "SALE_SOURCE_ID", "SOURCE_SYSTEM", "SOURCE_ENTITY") 
    DO NOTHING;
	 GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    CALL BL_CL.LOG_ETL_DM('LOAD_FCT_SALES_DD', rows_inserted, 'SUCCESS: Loaded data into correct partitions');
    
EXCEPTION WHEN OTHERS THEN CALL BL_CL.LOG_ETL_DM('LOAD_FCT_SALES_DD', 0, 'ERROR: ' || SQLERRM);
RAISE;
END;
$$;