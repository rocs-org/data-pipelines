-- depends: 20210802_01_create_schema_coronacases

CREATE TABLE coronacases.german_counties_more_info (
    caseid SERIAL PRIMARY KEY,
    stateid INT NOT NULL,
    state VARCHAR(128) NOT NULL,
    countyid INT NOT NULL,
    county VARCHAR(128) NOT NULL,
    agegroup VARCHAR(16) NOT NULL,
    agegroup2 VARCHAR(16) DEFAULT NULL,
    sex VARCHAR(10) DEFAULT NULL,
    date_cet TIMESTAMP [0] NOT NULL,
    ref_date_cet TIMESTAMP [0] NOT NULL,
    ref_date_is_symptom_onset boolean NOT NULL,
    is_new_case int DEFAULT NULL,
    is_new_death int DEFAULT NULL,
    is_new_recovered int DEFAULT NULL,
    new_cases int DEFAULT NULL,
    new_deaths int DEFAULT NULL,
    new_recovereds int DEFAULT NULL );