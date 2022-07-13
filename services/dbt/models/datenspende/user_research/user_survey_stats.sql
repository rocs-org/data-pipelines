{{
    config(
        materialized='table',
        tags=['weekly']
    )
}}

with user_survey_stats_pre as (

SELECT user_id,
       to_timestamp(completed_at/1000)::date    AS date
FROM   {{source('datenspende', 'questionnaire_session')}}

)

SELECT user_id,
       Min(date)                AS first_survey,
       Max(date)                AS last_survey,
       CURRENT_DATE - Max(date) AS days_since_survey,
       Count(DISTINCT date)     AS days_with_survey,
       Count(DISTINCT date) filter (WHERE date > (CURRENT_DATE - INTERVAL '30 days')) AS survey_last_30,
       Count(DISTINCT date) filter (WHERE date > (CURRENT_DATE - INTERVAL '60 days')) AS survey_last_60,
       Count(DISTINCT date) filter (WHERE date > (CURRENT_DATE - INTERVAL '90 days')) AS survey_last_90
FROM   user_survey_stats_pre
GROUP  BY user_id