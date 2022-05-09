BEGIN;

CREATE TABLE IF NOT EXISTS event ( 
    id SERIAL PRIMARY KEY,
    client varchar(255),
    client_version varchar(255),
    data_center varchar(255),
    processed_time timestamp,
    data JSONB );

COMMIT;