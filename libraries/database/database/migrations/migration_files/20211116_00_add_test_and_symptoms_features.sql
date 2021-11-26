--
-- depends: 20211025_00_add_datenspende_derivatives_schema

CREATE TABLE datenspende_derivatives.homogenized_features (
    user_id INTEGER NOT NULL,
    test_week_start DATE NOT NULL,
    f10 BOOLEAN,
    f40 BOOLEAN DEFAULT false,
    f41 BOOLEAN DEFAULT false,
    f42 BOOLEAN DEFAULT false,
    f44 BOOLEAN DEFAULT false,
    f45 BOOLEAN DEFAULT false,
    f46 BOOLEAN DEFAULT false,
    f47 BOOLEAN DEFAULT false,
    f48 BOOLEAN DEFAULT false,
    f49 BOOLEAN DEFAULT false,
    f74 INTEGER,
    f75 INTEGER,
    f76 INTEGER,
    f127 INTEGER,
    f133 INTEGER,
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

CREATE TABLE datenspende_derivatives.homogenized_features_description (
    description VARCHAR,
    id VARCHAR PRIMARY KEY,
    is_choice BOOLEAN
);
COMMENT ON TABLE
    datenspende_derivatives.homogenized_features_description
IS
    'Description of columns in homogenized features'
;