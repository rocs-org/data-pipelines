create materialized view
	datenspende_derivatives.vitals_standardized_by_date_and_user_before_infection
AS
select
	vital.user_id, vital.date, vital.type, vital.source,
	(vital.standardized_value - statistics.mean_from_standardized)/statistics.std_from_standardized standardized_value,
	(vital.standardized_value - statistics.mean_from_standardized) value_minus_mean_from_standardized,
	(vital.value_minus_mean - statistics.mean_from_subtracted_mean)/statistics.std_from_subtracted_mean standardized_value_from_value_minus_mean,
	(vital.value_minus_mean - statistics.mean_from_subtracted_mean) value_minus_mean_from_value_minus_mean
from
	datenspende_derivatives.vitals_standardized_by_daily_aggregates vital,
	datenspende_derivatives.vital_stats_before_infection_from_vitals_standardized_by_day statistics
where
    vital.user_id = statistics.user_id AND
    vital.source = statistics.source AND
    vital.type = statistics.type;

COMMENT ON COLUMN
	datenspende_derivatives.vitals_standardized_by_date_and_user_before_infection.standardized_value
IS
    '1) calculate average and standard deviation over users for equal date, source and type'
    '2) subtract average and divide by standard deviation to correct for annual cyclic drift and changes in wearable software '
    '3) calculate average and standard deviation over dates before first infection (if not infected, over all dates) for equal user source and type, '
    '4) subtract average and divide by standard deviation to correct for individually different baseline levels in vitals.';

COMMENT ON COLUMN
	datenspende_derivatives.vitals_standardized_by_date_and_user_before_infection.value_minus_mean_from_standardized
IS
    '1) calculate average and standard deviation over users for equal date, source and type'
    '2) subtract average and divide by standard deviation to correct for annual cyclic drift and changes in wearable software '
    '3) average over dates before first infection (if not infected, over all dates) for equal user source and type, '
    '4) subtract average to correct for individually different baseline levels in vitals.';

COMMENT ON COLUMN
	datenspende_derivatives.vitals_standardized_by_date_and_user_before_infection.standardized_value_from_value_minus_mean
IS
    '1) calculate average and standard deviation over users for equal date, source and type'
    '2) subtract average deviation to correct for annual cyclic drift and changes in wearable software '
    '3) calculate average and standard deviation over dates before first infection (if not infected, over all dates) for equal user source and type, '
    '4) subtract average and divide by standard deviation to correct for individually different baseline levels in vitals.';

COMMENT ON COLUMN
	datenspende_derivatives.vitals_standardized_by_date_and_user_before_infection.value_minus_mean_from_value_minus_mean
IS
    '1) calculate average and standard deviation over users for equal date, source and type'
    '2) subtract average to correct for annual cyclic drift and changes in wearable software '
    '3) average over dates before first infection (if not infected, over all dates) for equal user source and type, '
    '4) subtract average to correct for individually different baseline levels in vitals.'
