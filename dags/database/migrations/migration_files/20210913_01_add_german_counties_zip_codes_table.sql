CREATE TABLE censusdata.german_zip_codes (
    zip_code VARCHAR(255)  PRIMARY KEY,
    nuts VARCHAR(255) NOT NULL,
    CONSTRAINT fk_nuts FOREIGN KEY (nuts) REFERENCES censusdata.nuts (geo)
    );