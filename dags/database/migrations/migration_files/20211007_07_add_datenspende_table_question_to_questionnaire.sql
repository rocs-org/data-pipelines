CREATE TABLE datenspende.question_to_questionnaire (
    questionnaire INT,
    order_id INT,
    question INT,
    PRIMARY KEY (questionnaire, order_id, question)
);