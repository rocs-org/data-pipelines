CREATE TABLE datenspende_derivatives.daily_vital_rolling_window_time_series_features (
    user_id INT NOT NULL ,
    type SMALLINT NOT NULL ,
    source SMALLINT,
    date DATE NOT NULL ,
    fiftysix_day_mean_min_30_values FLOAT,
    fiftysix_day_min_min_30_values FLOAT,
    fiftysix_day_max_min_30_values FLOAT,
    fiftysix_day_median_min_30_values FLOAT,
    seven_day_mean_min_3_values FLOAT,
    seven_day_min_min_3_values FLOAT,
    seven_day_max_min_3_values FLOAT,
    seven_day_median_min_3_values FLOAT,
    CONSTRAINT one_value_per_user_day_and_type PRIMARY KEY (user_id, type, date)
);

COMMENT ON TABLE
    datenspende_derivatives.homogenized_features
IS
    'Different types of features derived from sliding window aggregation of daily vital data.'
;