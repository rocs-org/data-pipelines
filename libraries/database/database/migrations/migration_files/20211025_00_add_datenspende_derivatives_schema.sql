CREATE SCHEMA datenspende_derivatives;

CREATE TABLE datenspende_derivatives.test_and_symptoms_answers (
    user_id INT,
    question_id INT,
    session_id INT,
    answers VARCHAR,
    created_at BIGINT,
    PRIMARY KEY (user_id, question_id)
);

CREATE TABLE datenspende_derivatives.test_and_symptoms_answers_duplicates (
    user_id INT,
    question_id INT,
    session_id INT,
    answers VARCHAR,
    created_at BIGINT,
    PRIMARY KEY (user_id, question_id, created_at)
);
