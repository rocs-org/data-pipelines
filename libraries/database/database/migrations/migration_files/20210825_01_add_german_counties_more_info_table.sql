CREATE TABLE censusdata.german_counties_info (
    id SERIAL PRIMARY KEY,
    german_id INTEGER,
    regional_identifier VARCHAR(255) NOT NULL,
    municipality VARCHAR(255) NOT NULL,
    nuts VARCHAR(255) NOT NULL,
    area REAL NOT NULL,
    population INTEGER NOT NULL,
    m_population INTEGER NOT NULL,
    f_population INTEGER NOT NULL,
    population_density REAL NOT NULL,
    year DATE NOT NULL,
    CONSTRAINT fk_nuts FOREIGN KEY (nuts) REFERENCES censusdata.nuts (geo)
    );