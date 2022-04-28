DROP TABLE datenspende_derivatives.daily_vital_statistics;

create materialized view
	datenspende_derivatives.daily_vital_statistics
AS
select
	vital.user_id, vital.type, vital.source, avg(vital.value) mean, stddev(vital.value) std
from
	datenspende.vitaldata vital
where
    vital.user_id not in (
    select
        user_id
    from
        datenspende_derivatives.excluded_users
    where
        project = 'scripps colaboration long covid'
    )
group by vital.user_id , vital.type, vital.source;
