CREATE TABLE datenspende.questionnaire_session (
    id INT PRIMARY KEY,
    user_id INT,
    study INT,
    questionnaire INT,
    session_run INT,
    expiration_timestamp BIGINT,
    created_at BIGINT,
    completed_at BIGINT
);