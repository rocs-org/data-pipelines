ALTER TABLE datenspende_derivatives.daily_vital_rolling_window_time_series_features DROP CONSTRAINT one_value_per_user_day_and_type;
ALTER TABLE
    datenspende_derivatives.daily_vital_rolling_window_time_series_features
ADD CONSTRAINT
    one_value_per_user_day_and_type
PRIMARY KEY
    (user_id, type, source, date)
;