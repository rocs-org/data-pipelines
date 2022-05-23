{{ config(materialized='table') }}

with user_donations as (

SELECT user_id,
       Min(date)            AS first_vital,
       Max(date)            AS last_vital,
       Count(DISTINCT date) AS days_w_vital
FROM   datenspende.vitaldata
GROUP  BY user_id

)

select *
from user_donations