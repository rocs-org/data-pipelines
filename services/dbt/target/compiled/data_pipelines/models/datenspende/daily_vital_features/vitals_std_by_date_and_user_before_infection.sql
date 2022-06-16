

select
    vital.user_id, vital.date, vital.type, vital.source,
    (vital.standardized_value - statistics.mean_from_standardized)/statistics.std_from_standardized standardized_value,
    (vital.standardized_value - statistics.mean_from_standardized) value_minus_mean_from_standardized,
    (vital.value_minus_mean - statistics.mean_from_subtracted_mean)/statistics.std_from_subtracted_mean standardized_value_from_value_minus_mean,
    (vital.value_minus_mean - statistics.mean_from_subtracted_mean) value_minus_mean_from_value_minus_mean
from
    "rocs"."datenspende_derivatives"."vitals_standardized_by_daily_aggregates" vital,
    "rocs"."datenspende_derivatives"."agg_before_infection_from_vitals_std_by_day" statistics
where
        statistics.std_from_standardized > 0 AND
        statistics.std_from_subtracted_mean > 0 AND
        vital.user_id = statistics.user_id AND
        vital.source = statistics.source AND
        vital.type = statistics.type AND
        vital.user_id not in (
        select
            user_id
        from
            "rocs"."datenspende_derivatives"."excluded_users"
        where
                project = 'scripps colaboration long covid'
    )