CREATE TABLE datenspende_derivatives.excluded_users (
    reason TEXT NOT NULL,
    project TEXT,
    user_id INTEGER
);

INSERT INTO
    datenspende_derivatives.excluded_users (reason, project, user_id)
VALUES
    ('steps oulier', 'scripps colaboration long covid', 1213338);