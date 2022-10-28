CREATE TABLE raw_DynamicEpochValue
(
    `customer`       String,
    `type`           UInt16,
    `source`         UInt16,
    `startTimestamp` DateTime64(3, 'UTC'),
    `endTimestamp` Nullable(DateTime64(3, 'UTC')),
    `valueType` Enum8('DOUBLE' = 0, 'LONG' = 1, 'BOOLEAN' = 2, 'DATE' = 3, 'STRING' = 4),
    `doubleValue` Nullable(Float64),
    `longValue` Nullable(Int32),
    `booleanValue` Nullable(Enum8('false' = 0, 'true' = 1)),
    `dateValue` Nullable(DateTime64(3, 'UTC')),
    `stringValue` Nullable(String),
    `generation` Nullable(Enum8('MANUAL_ENTRY' = 0, 'MANUAL_MEASUREMENT' = 1, 'AUTOMATED_MEASUREMENT' = 2, 'CALCULATION' = 3, 'SMARTPHONE' = 4, 'TRACKER' = 5, 'THIRD_PARTY' = 6)),
    `trustworthiness` Nullable(Enum8('PLAUSIBLE' = 0, 'VERIFIED_FROM_DEVICE_SOURCE' = 1, 'VERIFIED_FROM_USER' = 2, 'VERIFIED_FROM_EXTERNAL_SOURCE' = 3, 'UNLIKELY' = 4, 'IMPLAUSIBLE' = 5, 'UNFAVORABLE_MEASUREMENT_CONTEXT' = 6, 'INSUFFICIENT_DATABASE' = 7, 'DOUBT_FROM_DEVICE_SOURCE' = 8, 'DOUBT_FROM_USER' = 9, 'DOUBT_FROM_EXTERNAL_SOURCE' = 10)),
    `medicalGrade` Nullable(Enum8('false' = 0, 'true' = 1)),
    `userReliability` Nullable(Enum8('AUTOMATICALLY_IDENTIFIED' = 0, 'MANUALLY_IDENTIFIED' = 1, 'CONFIRMED' = 2)),
    `chronologicalExactness` Nullable(Int16),
    `timezoneOffset` Nullable(Int16),
    `createdAt`      DateTime64(3, 'UTC'),
    `outdated` Enum8('false' = 0, 'true' = 1)
) ENGINE = MergeTree()
ORDER BY (startTimestamp, customer, type, source)