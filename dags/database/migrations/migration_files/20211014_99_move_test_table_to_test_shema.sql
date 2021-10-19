DROP TABLE test_table;
CREATE SCHEMA test_tables;
CREATE TABLE test_tables.test_table (
                id SERIAL PRIMARY KEY,
                col1 INT,
                col2 VARCHAR(255),
                col3 VARCHAR(255));