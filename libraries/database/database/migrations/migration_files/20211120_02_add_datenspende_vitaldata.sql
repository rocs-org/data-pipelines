--
-- depends: 20211007_00_add_datenspende_schema

CREATE TABLE datenspende.vitaldata (
    id BIGSERIAL PRIMARY KEY,
    user_id INT,
    date date,
    vital_id SMALLINT,
    value BIGINT,
    device_id SMALLINT,
    created_at BIGINT,
    timezone_offset SMALLINT,
    UNIQUE (user_id, date, vital_id, device_id),
    FOREIGN KEY (user_id) REFERENCES datenspende.users (user_id) ON DELETE CASCADE
    );