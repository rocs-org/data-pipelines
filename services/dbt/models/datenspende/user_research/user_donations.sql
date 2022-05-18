{{ config(materialized='table') }}

with user_donations as (

    SELECT user_id, MIN(date), MAX(date), COUNT(DISTINCT date)
    FROM datenspende.vitaldata
    GROUP BY (user_id)

)

select *
from user_donations
