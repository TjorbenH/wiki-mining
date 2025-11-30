-- script to be executed on the first startup of the postgres docker container 

CREATE TABLE IF NOT EXISTS RawData (
    link TEXT PRIMARY KEY,
    html TEXT
);


CREATE TABLE IF NOT EXISTS Links (
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    count integer default 1,
    PRIMARY KEY (origin, destination),
    CONSTRAINT fk_origin_rawdata
        FOREIGN KEY (origin)    
        REFERENCES RawData(link)
        ON DELETE CASCADE
);