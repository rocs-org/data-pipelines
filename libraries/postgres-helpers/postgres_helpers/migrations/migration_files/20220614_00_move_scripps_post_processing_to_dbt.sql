DO
$do$
BEGIN
    DROP MATERIALIZED VIEW IF EXISTS datenspende_derivatives.vitals_standardized_by_date_and_user_before_infection;

    DROP MATERIALIZED VIEW IF EXISTS datenspende_derivatives.vital_stats_before_infection_from_vitals_standardized_by_day;

    DROP MATERIALIZED VIEW IF EXISTS datenspende_derivatives.vitals_standardized_by_daily_aggregates;

    DROP MATERIALIZED VIEW IF EXISTS datenspende_derivatives.aggregates_for_standardization_by_type_source_date;

    DROP MATERIALIZED VIEW IF EXISTS datenspende_derivatives.daily_vital_statistics_before_infection;

    DROP MATERIALIZED VIEW IF EXISTS datenspende_derivatives.daily_vital_statistics;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error dropping views: %', SQLERRM;
END
$do$