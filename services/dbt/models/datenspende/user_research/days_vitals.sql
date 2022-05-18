{{ config(materialized='table') }}

with days_vitals as (

SELECT date, "type", COUNT(DISTINCT user_id) as daily_donors FROM datenspende.vitaldata GROUP BY ("date", "type")

)

select *
from days_vitals