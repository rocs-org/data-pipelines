CREATE TABLE censusdata.population (
    id SERIAL PRIMARY KEY,
    number INTEGER NOT NULL,
    nuts VARCHAR(255) NOT NULL,
    agegroup VARCHAR(16) NOT NULL,
    sex VARCHAR(16) NOT NULL,
    year INTEGER NOT NULL,
    data_quality_flags VARCHAR(255),
    CONSTRAINT fk_nuts FOREIGN KEY (nuts) REFERENCES censusdata.nuts (geo)
    );