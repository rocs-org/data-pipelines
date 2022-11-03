CREATE TABLE raw_DynamicValueType
(
    `this` Int32,
    `catch` Int32,
    `id` Int32,
    `code` Nullable(String),
    `name_de` Nullable(String),
    `name_en` Nullable(String),
    `name_es` Nullable(String),
    `name_fr` Nullable(String),
    `name_it` Nullable(String),
    `valueType` Int8,
    `level` Int32
)
    ENGINE = MergeTree()
    ORDER BY this