CREATE TABLE coronacases.german_counties_incidence (
    location_id INTEGER,
    location_level INTEGER,
    date_of_report TIMESTAMP NOT NULL,
    new_cases INTEGER,
    new_cases_last_7d FLOAT,
    incidence_7d_per_100k FLOAT,
    new_deaths INTEGER,
    nuts3 VARCHAR(255),
    population FLOAT NOT NULL,
    state FLOAT
    );