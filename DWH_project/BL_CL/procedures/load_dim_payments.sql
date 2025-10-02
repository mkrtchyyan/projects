--composite type
DO $$
BEGIN
    IF NOT EXISTS ( SELECT 1  FROM pg_type WHERE typname = 'payment_composite_type')
   THEN CREATE TYPE payment_composite_type AS ( "PAYMENT_SOURCE_ID" BIGINT  ,
                      											    "PAYMENT_METHOD_ID" BIGINT ,
                      											    "PAYMENT_METHOD_DESC" VARCHAR(255) ,
                      											    "PAYMENT_PROVIDER_ID" BIGINT ,
                      											    "PAYMENT_PROVIDER_DESC" VARCHAR(255) ,
                      											    "TRANSACTION_FEE" DECIMAL(8,2) ,
                      											    "TA_INSERT_DT" TIMESTAMP ,
                      											    "TA_UPDATE_DT" TIMESTAMP ,
                      											    "SOURCE_SYSTEM" VARCHAR(255) ,
                      											    "SOURCE_ENTITY" VARCHAR(255)); 
    end if;
end;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.load_dim_payments()
LANGUAGE plpgsql
AS $$
DECLARE
    rec payment_composite_type;  -- Composite type variable
    rows_affected INT := 0;
	  rows_inserted INT := 0;
    sql_stmt TEXT;
	
BEGIN
    FOR rec IN 
        SELECT 
            p."PAYMENT_ID" AS "PAYMENT_SOURCE_ID",
            p."PAYMENT_METHOD_ID" AS "PAYMENT_METHOD_ID",
            m."PAYMENT_METHOD_DESC" AS "PAYMENT_METHOD_DESC",
            p."PAYMENT_PROVIDER_ID" AS "PAYMENT_PROVIDER_ID",
            pr."PAYMENT_PROVIDER_DESC" AS "PAYMENT_PROVIDER_DESC",
            p."TRANSACTION_FEE" AS "TRANSACTION_FEE",
            CURRENT_TIMESTAMP AS "TA_INSERT_DT",
            CURRENT_TIMESTAMP AS "TA_UPDATE_DT",
            'BL_3NF' AS "SOURCE_SYSTEM",
            'CE_PAYMENTS' AS "SOURCE_ENTITY"
        FROM bl_3nf.ce_payments p
        LEFT JOIN bl_3nf.ce_payment_methods m ON p."PAYMENT_METHOD_ID" = m."PAYMENT_METHOD_ID"
        LEFT JOIN bl_3nf.ce_payment_providers pr ON p."PAYMENT_PROVIDER_ID" = pr."PAYMENT_PROVIDER_ID"
    LOOP
        sql_stmt := 'INSERT INTO bl_dm.dim_payments (
                        "PAYMENT_SURR_ID",
                        "PAYMENT_SOURCE_ID",
                        "PAYMENT_METHOD_ID",
                        "PAYMENT_METHOD_DESC",
                        "PAYMENT_PROVIDER_ID",
                        "PAYMENT_PROVIDER_DESC",
                        "TRANSACTION_FEE",
                        "TA_INSERT_DT",
                        "TA_UPDATE_DT",
                        "SOURCE_SYSTEM",
                        "SOURCE_ENTITY")
                    VALUES (  NEXTVAL(''bl_dm.dim_payments_seq''),
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT ("PAYMENT_SOURCE_ID","SOURCE_ENTITY","SOURCE_SYSTEM") DO NOTHING';

        EXECUTE sql_stmt
        USING
            rec."PAYMENT_SOURCE_ID",
            rec."PAYMENT_METHOD_ID",
            rec."PAYMENT_METHOD_DESC",
            rec."PAYMENT_PROVIDER_ID",
            rec."PAYMENT_PROVIDER_DESC",
            rec."TRANSACTION_FEE",
            rec."TA_INSERT_DT",
            rec."TA_UPDATE_DT",
            rec."SOURCE_SYSTEM",
            rec."SOURCE_ENTITY";
	    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
        rows_affected := rows_affected + rows_inserted;
    END LOOP;
    CALL bl_cl.log_etl_dm('LOAD_DIM_PAYMENTS', rows_affected, 'success');

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.log_etl_dm('LOAD_DIM_PAYMENTS', 0, 'ERROR: ' || SQLERRM);
    RAISE;
END;
$$;
