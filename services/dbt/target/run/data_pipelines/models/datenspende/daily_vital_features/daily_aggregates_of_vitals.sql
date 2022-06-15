

  create  table "rocs"."jakob"."daily_aggregates_of_vitals__dbt_tmp"
  as (
    

select
    vital.date, vital.type, vital.source, avg(vital.value) mean, stddev(vital.value) std
from
    "rocs"."datenspende"."vitaldata" vital
where vital.user_id not in (
    select
        user_id
    from
        "rocs"."datenspende_derivatives"."excluded_users"
    where
            project = 'scripps colaboration long covid'
)
group by vital.date , vital.type, vital.source
  );