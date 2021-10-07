CREATE TABLE datenspende.users (
    customer INT PRIMARY KEY,
    plz VARCHAR(5),
    salutation INT,
    birth_date INT,
    weight INT,
    height INT,
    creation_timestamp BIGINT,
    source INT
);