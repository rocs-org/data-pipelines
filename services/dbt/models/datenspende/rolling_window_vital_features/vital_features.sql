{{config(tags=['nowcast', 'tabular'])}}

SELECT
        features.user_id, signal.type, signal.source, features.date,
        (signal.mean - baseline.mean) / stats.std signal_mean,
        (signal.max - baseline.mean) / stats.std signal_max,
        (signal.min - baseline.mean) / stats.std signal_mean,
        signal.data_count signal_count, baseline.data_count baseline_count
FROM
    {{ ref('fifty_six_days')}} baseline,
    {{ ref('seven_days')}} signal,
    {{ ref('agg_before_infection_from_vitals_std_by_day') }} stats,
    (
        SELECT
            *
        FROM {{source('datenspende_derivatives', 'homogenized_features')}}
        ORDER BY user_id, test_week_start
    ) features
WHERE
  -- match vital types
        baseline.type = signal.type AND
        baseline.type = stats.type AND
  -- match source
        baseline.source = signal.source AND
        baseline.source = stats.source AND
  -- match user_ids
        features.user_id = baseline.user_id AND
        features.user_id = signal.user_id AND
        features.user_id = stats.user_id AND
  -- match dates
        features.test_week_start - integer '4' = baseline.window_end AND
        features.test_week_start - integer '3' = signal.window_start AND
  -- dont devide by zero
        stats.std > 0