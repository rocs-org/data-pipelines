{{config(tags=['scripps', 'tabular'])}}

select
    vital.date, vital.type, vital.source, avg(vital.value) mean, stddev(vital.value) std
from
    {{source('datenspende', 'vitaldata')}} vital
where vital.user_id not in (
    select
        user_id
    from
        {{source('datenspende_derivatives', 'excluded_users')}}
    where
            project = 'scripps colaboration long covid'
)
group by vital.date , vital.type, vital.source