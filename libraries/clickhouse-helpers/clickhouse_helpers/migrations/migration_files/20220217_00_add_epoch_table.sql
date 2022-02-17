CREATE TABLE vital_data_epoch
(
    customer INT,
    type smallint,
    valueType Int8,
    doubleValue double precision NULL,
    longValue INT NULL,
    booleanValue INT NULL,
    timezoneOffset smallint,
    startTimestamp bigint,
    endTimestamp bigint,
    createdAt bigint,
    source SMALLINT
)
ENGINE = ReplacingMergeTree()
ORDER BY (customer, type, startTimestamp, source)
PRIMARY KEY (customer, type, startTimestamp, source);

