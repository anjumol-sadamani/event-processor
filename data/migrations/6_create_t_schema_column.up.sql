BEGIN;
CREATE TABLE IF NOT EXISTS schema_column ( 
    id SERIAL PRIMARY KEY,
    query varchar(255)
);

COMMIT;