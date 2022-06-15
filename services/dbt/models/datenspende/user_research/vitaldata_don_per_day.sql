{{ config(materialized='table') }}

with vitaldata_don_per_day as (

SELECT   date                    AS cdate,
         "type"                  AS vital_type,
         Count(DISTINCT user_id) AS donors
FROM     {{source('datenspende', 'vitaldata')}}
GROUP BY cdate, vital_type

)

select *
from vitaldata_don_per_day