{{config(tags=['scripps', 'time-series'])}}

select
    vital.user_id, vital.date, vital.type, vital.source,
    (vital.value - statistics.mean) value_minus_mean,
    (vital.value - statistics.mean)/statistics.std standardized_value
from
    {{source('datenspende', 'vitaldata')}} vital, {{ref('daily_aggregates_of_vitals')}} statistics
where
        statistics.std > 0 AND
        vital.date = statistics.date AND
        vital.source = statistics.source AND
        vital.type = statistics.type AND
        vital.user_id not in (
        select
            user_id
        from
            {{source('datenspende_derivatives', 'excluded_users')}}
        where
                project = 'scripps colaboration long covid'
    );

COMMENT ON COLUMN
    datenspende_derivatives.vitals_standardized_by_daily_aggregates.standardized_value
    IS
        'standardized vitals over user, source and type.';

COMMENT ON COLUMN
    datenspende_derivatives.vitals_standardized_by_daily_aggregates.value_minus_mean
    IS
        'subtracted mean over user, source and type from vitals.'