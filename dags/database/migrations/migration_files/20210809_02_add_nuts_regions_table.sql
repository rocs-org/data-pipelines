-- 
-- depends: 20210809_01_add_census_schema

CREATE TABLE censusdata.nuts (
    id SERIAL PRIMARY KEY,
    level INTEGER,
    geo VARCHAR(16),
    name VARCHAR(255)
    );