CREATE TABLE raw_B2CCustomer
(
    `this` Int32,
    `catch` Int32,
    `id_email` Nullable(String),
    `id_emailLowerCase` Nullable(String),
    `activationCode` Nullable(String),
    `partnerUserID` Nullable(String),
    `company` Int32,
    `salutation` Nullable(Int8),
    `firstName` Nullable(String),
    `lastName` Nullable(String),
    `phoneNumber` Nullable(String),
    `language` Int8,
    `sharedData_emergencies` Int8,
    `sharedData_anomalies` Int8,
    `sharedData_location` Int8,
    `sharedData_activityData` Int8,
    `sharedData_friends` Int8,
    `sharedData_information` Int8,
    `notificatSettings_emergen` Int8,
    `notificatSettings_anomali` Int8,
    `notificatSettings_informa` Int8,
    `batteryWarning` Int8,
    `useActivDataForAnomaDetec` Int8,
    `deadManSwitchTriggered` Int8,
    `deadManSwitchPeriod` Nullable(Int8),
    `referenceGroup` Nullable(Int32),
    `password_jbcrypt` Nullable(String),
    `deviceID` Nullable(String),
    `appVersion` Nullable(String),
    `creationTimestamp` Int32,
    `birthDate` Nullable(Date),
    `weight` Nullable(Int32),
    `height` Nullable(Int32),
    `profilePicture_body` Nullable(String),
    `profilePicture_lastModifi` Nullable(Int32),
    `address_street` Nullable(String),
    `address_postalCode` Nullable(String),
    `address_city` Nullable(String),
    `address_country` Nullable(Int16),
    `pushErrors_Len` Int8,
    `pushErrors_0` Nullable(String),
    `pushErrors_1` Nullable(String),
    `pushErrors_2` Nullable(String),
    `termsAccepted` Int8,
    `privacyPolicyAccepted` Int8,
    `askedForDeletion` Int8,
    `blocked` Int8,
    `confirmed` Int8,
    `acceptedPolicyVersion` Int16
)
    ENGINE = MergeTree()
    ORDER BY this