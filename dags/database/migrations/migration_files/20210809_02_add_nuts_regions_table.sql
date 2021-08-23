-- 
-- depends: 20210809_01_add_census_schema

CREATE TABLE censusdata.nuts (
    level INTEGER,
    geo VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    country_id INTEGER
    );