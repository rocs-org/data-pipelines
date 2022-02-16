CREATE TABLE test_table
(
    id INT,
    col1 INT,
    col2 String,
    col3 String
)
ENGINE = MergeTree
ORDER BY col1;

