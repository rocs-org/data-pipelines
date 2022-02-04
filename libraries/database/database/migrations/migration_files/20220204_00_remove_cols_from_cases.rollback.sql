ALTER TABLE coronacases.german_counties_more_info
    ADD COLUMN agegroup2 VARCHAR(16) DEFAULT NULL,
    ADD COLUMN caseid SERIAL PRIMARY KEY;