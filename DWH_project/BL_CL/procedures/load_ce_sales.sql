CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_SALES()
LANGUAGE plpgsql
AS $$
DECLARE 
v_last_event_dt_online bigint;
v_last_event_dt_offline bigint;
v_last_transaction_id_online varchar(200);
v_last_transaction_id_offline varchar(200);
v_rows_inserted_1 INT := 0;
v_rows_inserted_2 INT := 0;
BEGIN
	SELECT COALESCE(last_event_dt,-1), coalesce(last_transaction_id,'n.a.')
	INTO v_last_event_dt_online,v_last_transaction_id_online
	FROM bl_cl.load_tracker
	where lower(source_type)='online';
	
	SELECT COALESCE(last_event_dt,-1), coalesce(last_transaction_id,'n.a.')
	INTO v_last_event_dt_offline,v_last_transaction_id_offline
	FROM bl_cl.load_tracker
	where lower(source_type)='offline';
-- Online sales
INSERT INTO BL_3NF.CE_SALES (
"SALE_ID",
"SALE_SRC_ID",
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
"PROFIT",
"PROFIT_MARGIN",
"DELIVERY_FEE",
"TA_INSERT_DT",
"SOURCE_SYSTEM",
"SOURCE_ENTITY"
)
SELECT 
nextval('BL_3NF.CE_SALES_SEQ'),
s."SALES_ID",
COALESCE(c."CUSTOMER_ID", -1),
-1, -- No staff for online sales
COALESCE(st."STORE_ID", -1),
COALESCE(p."PAYMENT_ID", -1),
-1, -- No manager for online sales
COALESCE(pr."PRODUCT_ID", -1),
s."EVENT_DT"::BIGINT,
s."QUANTITY"::INTEGER,
s."LINE_ITEM_AMOUNT"::DECIMAL(8,2),
s."COST"::DECIMAL(8,2),
s."PROFIT"::DECIMAL(8,2),
s."PROFIT_MARGIN"::DECIMAL(8,2),
s."DELIVERY_FEE"::DECIMAL(8,2),
CURRENT_TIMESTAMP,
'SA_ONLINE_SALES',
'SRC_ONLINE_COFFEE_SHOP_TRANSACTIONS'
FROM sa_online_sales.src_online_coffee_shop_transactions s
LEFT JOIN BL_CL.T_MAP_CUSTOMERS mp
ON mp."CUSTOMER_SRC_ID" = s."CUSTOMER_ID" 
AND mp."SOURCE_SYSTEM" = 'SA_ONLINE_SALES' 
AND mp."SOURCE_ENTITY" = 'SRC_ONLINE_COFFEE_SHOP_TRANSACTIONS'
LEFT JOIN BL_3NF.CE_CUSTOMERS_SCD c 
ON c."CUSTOMER_SRC_ID"::BIGINT = mp."CUSTOMER_ID"::BIGINT
AND c."IS_ACTIVE"=TRUE
LEFT JOIN BL_3NF.CE_STORES st 
ON st."STORE_SRC_ID" = s."STORE_ID" 
AND st."SOURCE_SYSTEM" = 'SA_ONLINE_SALES' 
AND st."SOURCE_ENTITY" = 'SRC_ONLINE_COFFEE_SHOP_TRANSACTIONS'
LEFT JOIN BL_3NF.CE_PAYMENTS p 
ON p."PAYMENT_SRC_ID"::BIGINT = s."PAYMENT_ID"::BIGINT 
AND p."SOURCE_SYSTEM" = 'SA_ONLINE_SALES' 
AND p."SOURCE_ENTITY" = 'SRC_ONLINE_COFFEE_SHOP_TRANSACTIONS'
LEFT JOIN BL_cl.t_map_products mpp
ON mpp."PRODUCT_SRC_ID" = s."PRODUCT_ID"
AND mpp."SOURCE_SYSTEM" = 'SA_ONLINE_SALES' 
AND mpp."SOURCE_ENTITY" = 'SRC_ONLINE_COFFEE_SHOP_TRANSACTIONS'
LEFT JOIN BL_3NF.CE_PRODUCTS pr
ON pr."PRODUCT_SRC_ID" = mpp."PRODUCT_ID"
where S."EVENT_DT"::bigint >v_last_event_dt_online or 
	  (S."EVENT_DT"::bigint =v_last_event_dt_online and S."SALES_ID" >v_last_transaction_id_online) 
--if i only write >= it might load the same data twice thats why its better to check for sales_id also
On conflict("SALE_SRC_ID","SOURCE_ENTITY","SOURCE_SYSTEM") DO NOTHING;
GET DIAGNOSTICS v_rows_inserted_1 = ROW_COUNT;
IF v_rows_inserted_1 > 0 THEN
INSERT INTO BL_CL.load_tracker(source_type,last_event_dt,last_transaction_id,last_updated)
SELECT 'ONLINE',
		coalesce(max(S."EVENT_DT")::bigint,-1),
		coalesce(max(S."SALES_ID"),'n.a.'),
		CURRENT_TIMESTAMP
FROM sa_online_sales.src_online_coffee_shop_transactions S
WHERE S."EVENT_DT"::bigint >v_last_event_dt_online or 
	  (S."EVENT_DT"::bigint =v_last_event_dt_online and S."SALES_ID" >v_last_transaction_id_online)
ON CONFLICT (source_type) do 
update set source_type=excluded.source_type,
		   last_event_dt=excluded.last_event_dt,
		   last_transaction_id=excluded.last_transaction_id,
		   last_updated=CURRENT_TIMESTAMP ;
end if;
-- Offline sales
INSERT INTO BL_3NF.CE_SALES (
"SALE_ID",
"SALE_SRC_ID",
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
"PROFIT",
"PROFIT_MARGIN",
"DELIVERY_FEE",
"TA_INSERT_DT",
"SOURCE_SYSTEM",
"SOURCE_ENTITY"
)
SELECT 
nextval('BL_3NF.CE_SALES_SEQ'),
s."SALES_ID",
COALESCE(c."CUSTOMER_ID", -1),
COALESCE(stf."STAFF_ID", -1),
COALESCE(st."STORE_ID", -1),
COALESCE(p."PAYMENT_ID", -1),
COALESCE(mgr."STAFF_ID", -1),
COALESCE(pr."PRODUCT_ID", -1),
s."EVENT_DT"::BIGINT,
s."QUANTITY"::INTEGER,
s."LINE_ITEM_AMOUNT"::DECIMAL(8,2),
s."COST"::DECIMAL(8,2),
s."PROFIT"::DECIMAL(8,2),
s."PROFIT_MARGIN"::DECIMAL(8,2),
0, -- Delivery fee
CURRENT_TIMESTAMP,
'SA_OFFLINE_SALES',
'SRC_OFFLINE_COFFEE_SHOP_TRANSACTIONS'
FROM sa_offline_sales.src_offline_coffee_shop_transactions s
LEFT JOIN BL_CL.T_MAP_CUSTOMERS mp
ON mp."CUSTOMER_SRC_ID" = s."CUSTOMER_ID" 
AND mp."SOURCE_SYSTEM" = 'SA_OFFLINE_SALES' 
AND mp."SOURCE_ENTITY" = 'SRC_OFFLINE_COFFEE_SHOP_TRANSACTIONS'
LEFT JOIN BL_3NF.CE_CUSTOMERS_SCD c 
ON c."CUSTOMER_SRC_ID"::BIGINT = mp."CUSTOMER_ID" 
AND c."IS_ACTIVE"=TRUE
LEFT JOIN BL_3NF.CE_STAFF stf 
ON stf."STAFF_SRC_ID" = s."STAFF_ID"
LEFT JOIN BL_3NF.CE_STORES st 
ON st."STORE_SRC_ID" = s."STORE_ID" 
AND st."SOURCE_SYSTEM" = 'SA_OFFLINE_SALES' 
AND st."SOURCE_ENTITY" = 'SRC_OFFLINE_COFFEE_SHOP_TRANSACTIONS'
LEFT JOIN BL_3NF.CE_PAYMENTS p 
ON p."PAYMENT_SRC_ID"::BIGINT = s."PAYMENT_ID"::BIGINT 
AND p."SOURCE_SYSTEM" = 'SA_OFFLINE_SALES' 
AND p."SOURCE_ENTITY" = 'SRC_OFFLINE_COFFEE_SHOP_TRANSACTIONS'
LEFT JOIN BL_3NF.CE_STAFF mgr 
ON mgr."STAFF_SRC_ID" = s."MANAGER_ID" 
LEFT JOIN BL_cl.t_map_products mpp
ON mpp."PRODUCT_SRC_ID" = s."PRODUCT_ID"
AND mpp."SOURCE_SYSTEM" = 'SA_OFFLINE_SALES' 
AND mpp."SOURCE_ENTITY" = 'SRC_OFFLINE_COFFEE_SHOP_TRANSACTIONS'
LEFT JOIN BL_3NF.CE_PRODUCTS pr
ON pr."PRODUCT_SRC_ID" = mpp."PRODUCT_ID"
where (S."EVENT_DT"::bigint >v_last_event_dt_offline) or 
	  (S."EVENT_DT"::bigint =v_last_event_dt_offline and S."SALES_ID" >v_last_transaction_id_offline) 
On conflict("SALE_SRC_ID","SOURCE_ENTITY","SOURCE_SYSTEM") DO NOTHING;
GET DIAGNOSTICS v_rows_inserted_2 = ROW_COUNT;
IF v_rows_inserted_2 > 0 THEN
INSERT INTO BL_CL.load_tracker(source_type,last_event_dt,last_transaction_id,last_updated)
SELECT 'OFFLINE',
		coalesce(max(S."EVENT_DT")::bigint,-1),
		coalesce(max(S."SALES_ID"),'n.a.'),
		CURRENT_TIMESTAMP
FROM sa_offline_sales.src_offline_coffee_shop_transactions S
WHERE S."EVENT_DT"::bigint >v_last_event_dt_offline or 
	  (S."EVENT_DT"::bigint =v_last_event_dt_offline and S."SALES_ID" >v_last_transaction_id_offline)
ON CONFLICT (source_type) do 
update set source_type=excluded.source_type,
		   last_event_dt=excluded.last_event_dt,
		   last_transaction_id=excluded.last_transaction_id,
		   last_updated=CURRENT_TIMESTAMP ;
end if;

CALL BL_CL.LOG_ETL('LOAD_CE_SALES', v_rows_inserted_1 + v_rows_inserted_2, 'SUCCESS');
EXCEPTION WHEN OTHERS THEN
CALL BL_CL.LOG_ETL('LOAD_CE_SALES', 0, 'ERROR: ' || SQLERRM);
END;
$$;