create materialized view
	datenspende_derivatives.aggregates_for_standardization_by_type_source_date
AS
select
	vital.date, vital.type, vital.source, avg(vital.value) mean, stddev(vital.value) std
from
	datenspende.vitaldata vital
group by vital.date , vital.type, vital.source;
