CREATE TABLE datenspende_derivatives.symptoms_features (
    user_id INTEGER,
    f451 BOOLEAN,
    f452 BOOLEAN,
    f453 BOOLEAN,
    f467 BOOLEAN,
    f470 BOOLEAN,
    f471 BOOLEAN,
    f472 BOOLEAN,
    f473 BOOLEAN,
    f476 BOOLEAN,
    f477 BOOLEAN,
    f478 BOOLEAN,
    f479 BOOLEAN,
    f74 REAL,
    f75 REAL,
    f76 REAL,
    f83 REAL,
    f127 REAL,
    f133 REAL
);

CREATE TABLE datenspende_derivatives.symptoms_features_description (
    description VARCHAR,
    id VARCHAR PRIMARY KEY,
    is_choice BOOLEAN
);

CREATE TABLE datenspende_derivatives.weekly_features (
    user_id INTEGER,
    f451 BOOLEAN,
    f452 BOOLEAN,
    f453 BOOLEAN,
    f467 BOOLEAN,
    f470 BOOLEAN,
    f471 BOOLEAN,
    f472 BOOLEAN,
    f473 BOOLEAN,
    f476 BOOLEAN,
    f477 BOOLEAN,
    f478 BOOLEAN,
    f479 BOOLEAN,
    f74 REAL,
    f75 REAL,
    f76 REAL,
    f83 REAL,
    f127 REAL,
    f133 REAL
);

CREATE TABLE datenspende_derivatives.weekly_features_description (
    description VARCHAR,
    id VARCHAR PRIMARY KEY,
    is_choice BOOLEAN
);