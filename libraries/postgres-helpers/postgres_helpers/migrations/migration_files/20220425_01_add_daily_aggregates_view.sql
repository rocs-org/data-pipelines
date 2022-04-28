create materialized view
	datenspende_derivatives.aggregates_for_standardization_by_type_source_date
AS
select
	vital.date, vital.type, vital.source, avg(vital.value) mean, stddev(vital.value) std
from
	datenspende.vitaldata vital
where vital.user_id not in (
    select
        user_id
    from
        datenspende_derivatives.excluded_users
    where
        project = 'scripps colaboration long covid'
    )
group by vital.date , vital.type, vital.source;
