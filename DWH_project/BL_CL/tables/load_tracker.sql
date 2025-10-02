CREATE TABLE IF NOT EXISTS bl_cl.load_tracker(
source_type varchar(20) PRIMARY KEY,
last_event_dt bigint NOT NULL,
last_transaction_id varchar(100) NOT NULL,
last_updated timestamp NOT NULL
);
--initial values
INSERT INTO bl_cl.load_tracker(source_type, last_event_dt, last_transaction_id, last_updated)
    VALUES ('ONLINE', -1, 'n.a.', CURRENT_TIMESTAMP),
           ('OFFLINE', -1, 'n.a.', CURRENT_TIMESTAMP);
