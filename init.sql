-- script to be executed on the first startup of the postgres docker container 

CREATE TABLE IF NOT EXISTS RawData (
    id BIGSERIAL PRIMARY KEY,
    link TEXT UNIQUE NOT NULL, 
    storage_key TEXT NOT NULL, -- path to the corresponding MINIO object
    content_hash VARCHAR(64), -- to ensure data integrity and help de-duplicate
);


CREATE TABLE IF NOT EXISTS Links (
    origin_id BIGINT NOT NULL,
    destination_id BIGINT NOT NULL,
    count INTEGER DEFAULT 1,
    PRIMARY KEY (origin_id, destination_id),
    CONSTRAINT fk_origin_rawdata
        FOREIGN KEY (origin_id)    
        REFERENCES RawData(id)
        ON DELETE CASCADE
);

-- CREATE INDEX IF NOT EXISTS idx_rawdata_link ON RawData(link);