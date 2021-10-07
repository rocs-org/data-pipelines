CREATE TABLE datenspende.questionnaires (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    description VARCHAR(255),
    hour_of_day_to_answer INT,
    expiration_in_minutes INT
);