-- script to be executed on the first startup of the postgres docker container 

CREATE TABLE IF NOT EXISTS RawData (
    id BIGSERIAL PRIMARY KEY,
    link TEXT UNIQUE NOT NULL, 
    storage_key TEXT, -- path to the corresponding MINIO object
    content_hash VARCHAR(64), -- to ensure data integrity and help de-duplicate
    scraping_status TEXT NOT NULL DEFAULT 'queued' 
        CHECK scraping_status IN ('queued', 'in_progress', 'done', 'failed'), -- ensure a failed scraping attempt doesn't loose the link but retries later
    attempts INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    
);


CREATE TABLE IF NOT EXISTS Links (
    origin_id BIGINT NOT NULL,
    destination_id BIGINT NOT NULL,
    PRIMARY KEY (origin_id, destination_id),
    CONSTRAINT fk_origin_rawdata
        FOREIGN KEY (origin_id)    
        REFERENCES RawData(id)
        ON DELETE CASCADE
);

-- CREATE INDEX idx_rawdata_status_updated ON RawData (status, updated_at); -- Index to allow for a timed sweep of stale entries