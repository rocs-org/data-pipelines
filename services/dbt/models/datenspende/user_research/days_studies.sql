{{ config(materialized='table') }}

with days_studies as (

SELECT to_timestamp(completed_at / 1000)::date, study, COUNT(user_id) FROM datenspende.questionnaire_session
WHERE completed_at IS NOT NULL
GROUP BY study, to_timestamp(completed_at / 1000)::date
ORDER BY to_timestamp(completed_at / 1000)::date

)

select *
from days_studies
