{{ config(materialized='table') }}

with surveys_per_day_pre as (

SELECT user_id,
       study,
       to_timestamp(completed_at/1000)::date    AS date
FROM   {{source('datenspende', 'questionnaire_session')}}

)

select date,
       study,
       COUNT(DISTINCT user_id) AS number_of_users
from surveys_per_day_pre
group by date, study