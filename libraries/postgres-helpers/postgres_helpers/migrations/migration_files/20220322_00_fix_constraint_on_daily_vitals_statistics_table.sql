ALTER TABLE datenspende_derivatives.daily_vital_statistics DROP CONSTRAINT one_value_per_user_and_type;
ALTER TABLE
    datenspende_derivatives.daily_vital_statistics
ADD CONSTRAINT
    one_value_per_user_and_type
PRIMARY KEY
    (user_id, type, source)
;