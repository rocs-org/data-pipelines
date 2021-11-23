CREATE TABLE datenspende_derivatives.symptoms_features (
    user_id INTEGER,
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
    f74 REAL,
    f75 REAL,
    f76 REAL,
    f83 REAL,
    f127 REAL,
    f133 REAL,
    f451 BOOLEAN,
    f467 BOOLEAN,
    f468 BOOLEAN,
    f469 BOOLEAN,
    f474 BOOLEAN,
    f478 BOOLEAN
);

CREATE TABLE datenspende_derivatives.symptoms_features_description (
    description VARCHAR,
    id VARCHAR PRIMARY KEY,
    is_choice BOOLEAN
);

CREATE TABLE datenspende_derivatives.weekly_features (
    user_id INTEGER,
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
    f74 REAL,
    f75 REAL,
    f76 REAL,
    f83 REAL,
    f127 REAL,
    f133 REAL,
    f451 BOOLEAN,
    f467 BOOLEAN,
    f468 BOOLEAN,
    f469 BOOLEAN,
    f474 BOOLEAN,
    f478 BOOLEAN
);

CREATE TABLE datenspende_derivatives.weekly_features_description (
    description VARCHAR,
    id VARCHAR PRIMARY KEY,
    is_choice BOOLEAN
);