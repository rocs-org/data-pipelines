CREATE TABLE raw_DynamicDataSource
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
    `enabled` Int8,
    `requiresPing` Int8,
    `logo_body` Nullable(String),
    `logo_contentType` Int8,
    `logo_lastModified` Int16,
    `enableLog` Int8
)
    ENGINE = MergeTree()
    ORDER BY this