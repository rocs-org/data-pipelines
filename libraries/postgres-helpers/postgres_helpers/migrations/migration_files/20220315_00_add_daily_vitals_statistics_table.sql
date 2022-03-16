CREATE TABLE datenspende_derivatives.daily_vital_statistics (
    user_id INT,
    type SMALLINT,
    source SMALLINT,
    std FLOAT,
    mean FLOAT,
    CONSTRAINT one_value_per_user_and_type PRIMARY KEY (user_id, type)
);