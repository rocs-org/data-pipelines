
CREATE TABLE datenspende_derivatives.homogenized_features (
    user_id INTEGER NOT NULL,
    test_week_start DATE NOT NULL,
    f10 BOOLEAN,
    f40 BOOLEAN,
    f41 BOOLEAN,
    f42 BOOLEAN,
    f44 BOOLEAN,
    f45 BOOLEAN,
    f46 BOOLEAN,
    f47 BOOLEAN,
    f48 BOOLEAN,
    f49 BOOLEAN,
    f74 INTEGER,
    f75 INTEGER,
    f76 INTEGER,
    f127 INTEGER,
    f133 INTEGER,
    f121 INTEGER,
    f467 BOOLEAN,
    f468 BOOLEAN,
    f469 BOOLEAN,
    f474 BOOLEAN,
    f478 BOOLEAN,
    CONSTRAINT unique_answers PRIMARY KEY (user_id, test_week_start)
);
COMMENT ON TABLE
    datenspende_derivatives.homogenized_features
IS
    'Test results, reported symptoms and info about body from weekly and one off surveys'
;


CREATE FUNCTION is_column_name_in_homogenized_features_table(id TEXT) RETURNS BOOLEAN AS
$func$
BEGIN
    RETURN id IN (
    SELECT
        column_name
    FROM
        information_schema.columns
    WHERE
        table_schema = 'datenspende_derivatives' AND
        TABLE_NAME = 'homogenized_features'
    );
END
$func$ LANGUAGE plpgsql;


CREATE TABLE datenspende_derivatives.homogenized_features_description (
    description VARCHAR,
    id VARCHAR PRIMARY KEY,
    is_choice BOOLEAN
    CONSTRAINT
        id_matches_feature_column_name
    CHECK (
        is_column_name_in_homogenized_features_table(id)
    )
);
COMMENT ON TABLE
    datenspende_derivatives.homogenized_features_description
IS
    'Description of columns in homogenized features'
;

