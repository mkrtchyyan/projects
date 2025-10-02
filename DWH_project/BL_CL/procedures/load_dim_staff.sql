CREATE OR REPLACE PROCEDURE bl_cl.load_dim_staff()
LANGUAGE plpgsql
AS $$
DECLARE  rows_inserted INT := 0;
BEGIN
    INSERT INTO bl_dm.dim_staff (
        "STAFF_SURR_ID",
        "STAFF_SOURCE_ID",
        "STAFF_FIRST_NAME",
        "STAFF_LAST_NAME",
        "JOB_START_DT",
        "EMPLOYMENT_STATUS",
        "HOURLY_WAGE",
        "JOB_ID",
        "JOB_DESC",
        "TA_INSERT_DT",
        "TA_UPDATE_DT",
        "SOURCE_SYSTEM",
        "SOURCE_ENTITY"
    )
    SELECT  nextval('bl_dm.dim_staff_seq'),
        st."STAFF_ID",
        st."STAFF_FIRST_NAME",
        st."STAFF_LAST_NAME",
        st."JOB_START_DT",
        st."EMPLOYMENT_STATUS",
        st."HOURLY_WAGE",
        j."JOB_ID",
        j."JOB_DESC",
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'BL_3NF',
        'CE_STAFF'
    FROM bl_3nf.ce_staff_jobs sj
    JOIN bl_3nf.ce_staff st ON sj."STAFF_ID" = st."STAFF_ID"
    JOIN bl_3nf.ce_jobs j ON sj."JOB_ID" = j."JOB_ID"
    on conflict("STAFF_SOURCE_ID","SOURCE_SYSTEM","SOURCE_ENTITY")
	do update set 
        "STAFF_FIRST_NAME"=excluded."STAFF_FIRST_NAME",
        "STAFF_LAST_NAME"=excluded. "STAFF_LAST_NAME",
        "JOB_START_DT"=excluded."JOB_START_DT",
        "EMPLOYMENT_STATUS"=excluded."EMPLOYMENT_STATUS",
        "HOURLY_WAGE"=excluded. "HOURLY_WAGE",
        "JOB_ID"=excluded."JOB_ID",
        "JOB_DESC"=excluded."JOB_DESC",
        "TA_UPDATE_DT"=current_timestamp
	where 
	   bl_dm.dim_staff."STAFF_FIRST_NAME" is distinct from excluded."STAFF_FIRST_NAME" or
        bl_dm.dim_staff."STAFF_LAST_NAME" is distinct from excluded. "STAFF_LAST_NAME" or
        bl_dm.dim_staff."JOB_START_DT" is distinct from excluded."JOB_START_DT" or
        bl_dm.dim_staff."EMPLOYMENT_STATUS" is distinct from excluded."EMPLOYMENT_STATUS" or
        bl_dm.dim_staff."HOURLY_WAGE" is distinct from excluded."HOURLY_WAGE" or
        bl_dm.dim_staff."JOB_ID" is distinct from excluded."JOB_ID" or
        bl_dm.dim_staff."JOB_DESC" is distinct from excluded."JOB_DESC";

    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    CALL bl_cl.log_etl_dm('LOAD_DIM_STAFF', rows_inserted, 'SUCCESS');

EXCEPTION WHEN OTHERS THEN  
CALL bl_cl.log_etl_dm('LOAD_DIM_STAFF', 0, 'ERROR: ' || SQLERRM);
    RAISE;
END;
$$;