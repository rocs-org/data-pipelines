--
-- depends: 20211007_00_add_datenspende_schema

CREATE TABLE datenspende.vitaldata (
    user_id INT,
    date date,
    type SMALLINT,
    value BIGINT,
    source SMALLINT,
    created_at BIGINT,
    timezone_offset SMALLINT,
    PRIMARY KEY (user_id, date, type, source),
    FOREIGN KEY (user_id) REFERENCES datenspende.users (user_id) ON DELETE CASCADE
    );