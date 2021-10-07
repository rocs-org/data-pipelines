CREATE TABLE datenspende.answers (
    id INT,
    user_id INT,
    questionnaire_session INT,
    study INT,
    questionnaire INT,
    question INT,
    order_id INT,
    created_at BIGINT,
    element INT,
    answer_text VARCHAR,
     PRIMARY KEY (id, element)
);