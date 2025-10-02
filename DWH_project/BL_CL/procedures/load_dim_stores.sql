CREATE OR REPLACE PROCEDURE bl_cl.load_dim_stores()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_processed INT := 0;
    rows_affected INT := 0;
    v_rowcount INT;
    rec RECORD;
BEGIN  FOR rec IN  SELECT
            s."STORE_ID",
            s."STORE_DESC",
            s."WEBSITE",
            c."CITY_ID",
            c."CITY_DESC",
            co."COUNTRY_ID",
            co."COUNTRY_DESC",
            s."STORE_ADDRESS",
            s."STORE_TYPE",
            s."TA_INSERT_DT",
            CURRENT_TIMESTAMP AS "TA_UPDATE_DT",
            'BL_3NF' AS "SOURCE_SYSTEM",
            'CE_STORES' AS "SOURCE_ENTITY"
        FROM bl_3nf.ce_stores s
        JOIN bl_3nf.ce_cities c ON s."CITY_ID" = c."CITY_ID"
        JOIN bl_3nf.ce_countries co ON c."COUNTRY_ID" = co."COUNTRY_ID"
    LOOP
        INSERT INTO bl_dm.dim_stores (
            "STORE_SURR_ID",
            "STORE_SOURCE_ID",
            "STORE_DESC",
            "WEBSITE",
            "CITY_ID",
            "CITY_DESC",
            "COUNTRY_ID",
            "COUNTRY_DESC",
            "STORE_ADDRESS",
            "STORE_TYPE",
            "TA_INSERT_DT",
            "TA_UPDATE_DT",
            "SOURCE_SYSTEM",
            "SOURCE_ENTITY"
        )
        VALUES (
            nextval('bl_dm.dim_stores_seq'),
            rec."STORE_ID",
            rec."STORE_DESC",
            rec."WEBSITE",
            rec."CITY_ID",
            rec."CITY_DESC",
            rec."COUNTRY_ID",
            rec."COUNTRY_DESC",
            rec."STORE_ADDRESS",
            rec."STORE_TYPE",
            rec."TA_INSERT_DT",
            rec."TA_UPDATE_DT",
            rec."SOURCE_SYSTEM",
            rec."SOURCE_ENTITY"
        )
        ON CONFLICT ("STORE_SOURCE_ID", "SOURCE_SYSTEM", "SOURCE_ENTITY")
        DO update set 
						"CITY_ID"=excluded."CITY_ID",
						"WEBSITE"=excluded."WEBSITE",
						"STORE_DESC"=excluded."STORE_DESC",
						"STORE_ADDRESS"=excluded."STORE_ADDRESS",
						"CITY_DESC"=excluded."CITY_DESC",
						"COUNTRY_ID"=excluded."COUNTRY_ID",
            			"COUNTRY_DESC"=excluded."COUNTRY_DESC",
						"STORE_TYPE"=excluded."STORE_TYPE",
						"TA_UPDATE_DT"=current_timestamp

				  where  
						bl_dm.dim_stores."CITY_ID" is distinct from excluded."CITY_ID" or
						bl_dm.dim_stores."WEBSITE" is distinct from excluded."WEBSITE" or
						bl_dm.dim_stores."STORE_DESC" is distinct from excluded."STORE_DESC" or
						bl_dm.dim_stores."STORE_ADDRESS" is distinct from excluded."STORE_ADDRESS" or
						bl_dm.dim_stores."CITY_DESC" is distinct from excluded."CITY_DESC" or
						bl_dm.dim_stores."COUNTRY_ID" is distinct from excluded."COUNTRY_ID" or
						bl_dm.dim_stores."COUNTRY_DESC" is distinct from excluded."COUNTRY_DESC" or
						bl_dm.dim_stores."STORE_TYPE" is distinct from excluded."STORE_TYPE" ;

        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        rows_affected := rows_affected + v_rowcount;
        rows_processed := rows_processed ;
    END LOOP;

    CALL bl_cl.log_etl_dm('LOAD_DIM_STORES', rows_affected, 'success');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.log_etl_dm('LOAD_DIM_STORES', 0, 'ERROR: ' || SQLERRM);
    RAISE;
END;
$$;