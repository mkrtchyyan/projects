CREATE OR REPLACE PROCEDURE bl_cl.load_dim_products()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_inserted INTEGER := 0;
    v_rows INTEGER;
    rec RECORD;
BEGIN
    FOR rec IN  SELECT 
            cp."PRODUCT_ID",
            cp."PRODUCT_DESC",
            cp."PRODUCT_TYPE_ID",
            pt."PRODUCT_TYPE_DESC",
            pc."PRODUCT_CATEGORY_ID",
            pc."PRODUCT_CATEGORY_DESC",
            pg."PRODUCT_GROUP_ID",
            pg."PRODUCT_GROUP_DESC",
            cp."PROMO_FLAG",
            cp."PROMO_DISCOUNT",
            cp."BASE_PRICE",
            cp."IS_LIMITED_EDITION",
            current_timestamp as "TA_INSERT_DT",
            current_timestamp AS "TA_UPDATE_DT",
            'BL_3NF' AS "SOURCE_SYSTEM",
            'CE_PRODUCTS' AS "SOURCE_ENTITY"
        FROM bl_3nf.ce_products cp
        JOIN bl_3nf.ce_product_types pt ON cp."PRODUCT_TYPE_ID" = pt."PRODUCT_TYPE_ID"
        JOIN bl_3nf.ce_product_categories pc ON pt."PRODUCT_CATEGORY_ID" = pc."PRODUCT_CATEGORY_ID"
        JOIN bl_3nf.ce_product_groups pg ON pc."PRODUCT_GROUP_ID" = pg."PRODUCT_GROUP_ID"
    LOOP
        INSERT INTO bl_dm.dim_products (
            "PRODUCT_SURR_ID",
            "PRODUCT_SOURCE_ID",
            "PRODUCT_DESC",
            "PRODUCT_TYPE_ID",
            "PRODUCT_TYPE_DESC",
            "PRODUCT_CATEGORY_ID",
            "PRODUCT_CATEGORY_DESC",
            "PRODUCT_GROUP_ID",
            "PRODUCT_GROUP_DESC",
            "PROMO_FLAG",
            "PROMO_DISCOUNT",
            "BASE_PRICE",
            "IS_LIMITED_EDITION",
            "TA_INSERT_DT",
            "TA_UPDATE_DT",
            "SOURCE_SYSTEM",
            "SOURCE_ENTITY"
        )
        VALUES (
            nextval('bl_dm.dim_products_seq'),
            rec."PRODUCT_ID",
            rec."PRODUCT_DESC",
            rec."PRODUCT_TYPE_ID",
            rec."PRODUCT_TYPE_DESC",
            rec."PRODUCT_CATEGORY_ID",
            rec."PRODUCT_CATEGORY_DESC",
            rec."PRODUCT_GROUP_ID",
            rec."PRODUCT_GROUP_DESC",
            rec."PROMO_FLAG",
            rec."PROMO_DISCOUNT",
            rec."BASE_PRICE",
            rec."IS_LIMITED_EDITION",
            rec."TA_INSERT_DT",
            rec."TA_UPDATE_DT",
            rec."SOURCE_SYSTEM",
            rec."SOURCE_ENTITY"
        )
        ON CONFLICT ("PRODUCT_SOURCE_ID", "SOURCE_SYSTEM", "SOURCE_ENTITY")
        DO  update set
            "PRODUCT_DESC"=excluded."PRODUCT_DESC",
            "PRODUCT_TYPE_ID"=excluded."PRODUCT_TYPE_ID",
            "PRODUCT_TYPE_DESC"=excluded."PRODUCT_TYPE_DESC",
            "PRODUCT_CATEGORY_ID"=excluded."PRODUCT_CATEGORY_ID",
            "PRODUCT_CATEGORY_DESC"=excluded."PRODUCT_CATEGORY_DESC",
            "PRODUCT_GROUP_ID"=excluded."PRODUCT_GROUP_ID",
            "PRODUCT_GROUP_DESC"=excluded."PRODUCT_GROUP_DESC",
            "PROMO_FLAG"=excluded."PROMO_FLAG",
            "PROMO_DISCOUNT"=excluded."PROMO_DISCOUNT",
            "BASE_PRICE"=excluded."BASE_PRICE",
            "IS_LIMITED_EDITION"=excluded."IS_LIMITED_EDITION",
            "TA_UPDATE_DT"=current_timestamp 

where   bl_dm.dim_products."PRODUCT_DESC" is distinct  from excluded."PRODUCT_DESC" or
		bl_dm.dim_products."PRODUCT_TYPE_ID" is distinct  from excluded."PRODUCT_TYPE_ID" or
		bl_dm.dim_products."PRODUCT_TYPE_DESC" is distinct  from excluded."PRODUCT_TYPE_DESC" or
		bl_dm.dim_products."PRODUCT_CATEGORY_ID" is distinct  from excluded."PRODUCT_CATEGORY_ID" or
		bl_dm.dim_products."PRODUCT_CATEGORY_DESC" is distinct  from excluded."PRODUCT_CATEGORY_DESC" or
		bl_dm.dim_products."PRODUCT_GROUP_ID" is distinct  from excluded."PRODUCT_GROUP_ID" or
		bl_dm.dim_products."PRODUCT_GROUP_DESC" is distinct  from excluded."PRODUCT_GROUP_DESC" or
		bl_dm.dim_products."PROMO_FLAG" is distinct  from excluded."PROMO_FLAG" or
		bl_dm.dim_products."PROMO_DISCOUNT" is distinct  from excluded."PROMO_DISCOUNT" or
		bl_dm.dim_products."BASE_PRICE" is distinct  from excluded."BASE_PRICE" or
		bl_dm.dim_products."IS_LIMITED_EDITION" is distinct  from excluded."IS_LIMITED_EDITION" ;

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        rows_inserted := rows_inserted + v_rows;
    END LOOP;
    CALL bl_cl.log_etl_dm('LOAD_DIM_PRODUCTS', rows_inserted, 'SUCCESS');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.log_etl_dm('LOAD_DIM_PRODUCTS', 0, 'ERROR: ' || SQLERRM);
    RAISE;
END;
$$;
