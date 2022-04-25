DROP TABLE datenspende_derivatives.daily_vital_statistics;

create materialized view
	datenspende_derivatives.daily_vital_statistics
AS
select
	vital.user_id, vital.type, vital.source, avg(vital.value) mean, stddev(vital.value) std
from
	datenspende.vitaldata vital
group by vital.user_id , vital.type, vital.source;
