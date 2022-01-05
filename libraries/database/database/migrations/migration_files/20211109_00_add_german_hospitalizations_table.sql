CREATE TABLE coronacases.german_hospitalizations (
    id SERIAL PRIMARY KEY,
    date_updated TIMESTAMP NOT NULL,
    new_hospitalizations INT NOT NULL,
    new_hospitalizations_per_100k FLOAT NOT NULL,
    year INT DEFAULT NULL,
    calendar_week SMALLINT DEFAULT NULL,
    year_week VARCHAR(10) NOT NULL DEFAULT '',
    date_begin DATE NOT NULL,
    date_end DATE NOT NULL,
    days_since_jan1_begin SMALLINT NOT NULL,
    days_since_jan1_end SMALLINT NOT NULL,
    UNIQUE (date_updated, year_week, new_hospitalizations)
);
