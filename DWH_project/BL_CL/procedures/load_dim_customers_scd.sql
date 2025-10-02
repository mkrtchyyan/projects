CREATE OR REPLACE PROCEDURE bl_cl.load_dim_customers_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_inserted INT := 0;
BEGIN
     MERGE INTO bl_dm.dim_customers_scd tgt
 USING (SELECT cust.*,
 			   c."COUNTRY_DESC"
 			   FROM bl_3nf.ce_customers_scd cust
 			   LEFT JOIN bl_3nf.ce_countries c
 			   ON cust."COUNTRY_ID"=c."COUNTRY_ID") src
 ON  tgt."CUSTOMER_FIRST_NAME"=src."CUSTOMER_FIRST_NAME"
 AND tgt."CUSTOMER_LAST_NAME"=src."CUSTOMER_LAST_NAME"
 AND tgt."COUNTRY_ID"=src."COUNTRY_ID"
 AND tgt."LOYALTY_MEMBER"=src."LOYALTY_MEMBER"
 AND tgt."LOYALTY_DISCOUNT"=src."LOYALTY_DISCOUNT"
 AND tgt. "GENDER"=src."GENDER"
 AND tgt."BIRTH_YEAR"=src."BIRTH_YEAR"
 AND tgt."SIGNUP_DT"=src."SIGNUP_DT"
 
WHEN MATCHED AND src."IS_ACTIVE"=FALSE AND tgt."IS_ACTIVE"=TRUE THEN
UPDATE SET "IS_ACTIVE"=FALSE,
 		   "END_DT"='9999-01-31',
       "TA_UPDATE_DT"=current_timestamp
WHEN NOT MATCHED and src."IS_ACTIVE"=TRUE then
INSERT ("CUSTOMER_SURR_ID",
        "CUSTOMER_SOURCE_ID",
        "CUSTOMER_FIRST_NAME",
        "CUSTOMER_LAST_NAME",
        "COUNTRY_ID",
        "COUNTRY_DESC",
        "LOYALTY_MEMBER",
        "LOYALTY_DISCOUNT",
        "GENDER",
        "BIRTH_YEAR",
        "SIGNUP_DT",
        "START_DT",
        "END_DT",
        "IS_ACTIVE",
        "TA_INSERT_DT",
        "TA_UPDATE_DT",
        "SOURCE_SYSTEM",
        "SOURCE_ENTITY")
        VALUES ( nextval('bl_dm.dim_customers_scd_seq'),
        src."CUSTOMER_ID",
        src."CUSTOMER_FIRST_NAME",
        src."CUSTOMER_LAST_NAME",
        src."COUNTRY_ID",
        src."COUNTRY_DESC",
        src."LOYALTY_MEMBER",
        src."LOYALTY_DISCOUNT",
        src."GENDER",
        src."BIRTH_YEAR",
        src."SIGNUP_DT",
        src."START_DT",
        src."END_DT",
        src."IS_ACTIVE",
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'BL_3NF',
        'CE_CUSTOMERS_SCD');
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;

    CALL bl_cl.log_etl_dm('LOAD_DIM_CUSTOMERS_SCD', rows_inserted, 'SUCCESS');

EXCEPTION WHEN OTHERS THEN  CALL bl_cl.log_etl_dm('LOAD_DIM_CUSTOMERS_SCD', 0, 'ERROR: ' || SQLERRM);
    RAISE;
END;
$$;
