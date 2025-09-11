{% macro log_bronze_run(model_name) %}
    INSERT INTO logs.dbt_logs (
        log_id, dataset, time_processed, source_records, target_records,
        bad_records, model_name, run_id, status, execution_time_seconds, created_at
    )
    SELECT
        cast(unix_timestamp() as bigint),
        '{{ model_name }}',
        current_timestamp(),
        NULL,
        (SELECT count(*) FROM {{ ref(model_name) }}),
        0,
        '{{ model_name }}',
        '{{ invocation_id }}',
        'SUCCESS',
        NULL,
        current_timestamp()
{% endmacro %}
