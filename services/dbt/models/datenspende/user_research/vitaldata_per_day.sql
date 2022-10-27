{{ config(materialized='table') }}

with vitals_per_day as (

SELECT   date,
         "type"                  AS vital_type,
         Count(DISTINCT user_id) AS number_of_users
FROM     {{source('datenspende', 'vitaldata')}}
GROUP BY date, vital_type

)

select *
from vitals_per_day