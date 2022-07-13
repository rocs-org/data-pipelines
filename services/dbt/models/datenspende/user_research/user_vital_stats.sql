{{
    config(
        materialized='table',
        tags=['weekly']
    )
}}

with user_vital_stats_pre as (

SELECT user_id,
       date
FROM   {{source('datenspende', 'vitaldata')}}

)

SELECT user_id,
       Min(date)                AS first_vital,
       Max(date)                AS last_vital,
       CURRENT_DATE - Max(date) AS days_since_vital,
       Count(DISTINCT date)     AS days_with_vital,
       Count(DISTINCT date) filter (WHERE date > (CURRENT_DATE - INTERVAL '30 days')) AS vital_last_30,
       Count(DISTINCT date) filter (WHERE date > (CURRENT_DATE - INTERVAL '60 days')) AS vital_last_60,
       Count(DISTINCT date) filter (WHERE date > (CURRENT_DATE - INTERVAL '90 days')) AS vital_last_90
FROM   user_vital_stats_pre
GROUP  BY user_id