{{config(tags=['scripps', 'tabular'])}}

select
    vital.user_id, vital.type, vital.source,
    avg(vital.standardized_value) mean_from_standardized, stddev(vital.standardized_value) std_from_standardized,
    avg(vital.value_minus_mean) mean_from_subtracted_mean, stddev(vital.value_minus_mean) std_from_subtracted_mean
from
    {{ref('vitals_standardized_by_daily_aggregates')}} vital,
    (
        -- select four days before the week during which users first reported their first infection
        -- for users who report at least one positive test
        (select distinct on (user_id)
             user_id, (test_week_start - INTERVAL '4 DAYS') test_week_start
         from
             {{source('datenspende_derivatives', 'homogenized_features')}}
             -- f10 is test result
         where f10 is true
         order by user_id, test_week_start)
        UNION
        -- use todays date for users who dont report a positive test
        (select distinct on (user_id)
             user_id, current_date test_week_start
         from
             {{source('datenspende_derivatives', 'homogenized_features')}}
             -- users that don't report a positive test result
         where user_id not in (
             select
                 user_id
             from
                 {{source('datenspende_derivatives', 'homogenized_features')}}
             where
                 f10 is true
         )
        )
    ) before_test
where
  -- select vitals before first positive test if there was a positive test, else select all up until today

        vital.date < before_test.test_week_start and
        vital.user_id = before_test.user_id AND
        vital.user_id not in (
        select
            user_id
        from
            {{source('datenspende_derivatives', 'excluded_users')}}
        where
                project = 'scripps colaboration long covid'
    )
group by vital.user_id , vital.type, vital.source
