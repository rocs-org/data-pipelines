CREATE SCHEMA datenspende;

CREATE TABLE datenspende.users (
    user_id INT PRIMARY KEY,
    plz VARCHAR(5),
    salutation INT,
    birth_date INT,
    weight INT,
    height INT,
    creation_timestamp BIGINT,
    source INT
);

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
    PRIMARY KEY (id, element),
    FOREIGN KEY (user_id) REFERENCES datenspende.users (user_id) ON DELETE CASCADE
);

CREATE TABLE datenspende.choice (
    element INT PRIMARY KEY,
    question INT,
    choice_id INT,
    text VARCHAR(255)
    );

CREATE TABLE datenspende.questionnaires (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    description VARCHAR(255),
    hour_of_day_to_answer INT,
    expiration_in_minutes INT
);

CREATE TABLE datenspende.questionnaire_session (
    id INT PRIMARY KEY,
    user_id INT,
    study INT,
    questionnaire INT,
    session_run INT,
    expiration_timestamp BIGINT,
    created_at BIGINT,
    completed_at BIGINT,
    FOREIGN KEY (user_id) REFERENCES datenspende.users (user_id) ON DELETE CASCADE
);

CREATE TABLE datenspende.questions (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    text VARCHAR,
    description VARCHAR,
    type INT
);

CREATE TABLE datenspende.question_to_questionnaire (
    questionnaire INT,
    order_id INT,
    question INT,
    PRIMARY KEY (questionnaire, order_id, question),
    FOREIGN KEY (questionnaire) REFERENCES datenspende.questionnaires (id) ON DELETE CASCADE,
    FOREIGN KEY (question) REFERENCES datenspende.questions (id) ON DELETE CASCADE
);

CREATE TABLE datenspende.study_overview (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    short_description VARCHAR(255)
);