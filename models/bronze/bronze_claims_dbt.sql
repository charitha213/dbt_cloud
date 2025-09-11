{{ config(
    materialized='table',
    schema='bronze',
    post_hook=[
        "INSERT INTO logs.dbt_logs (dataset, time_processed, source_records, target_records, bad_records, model_name, run_id, status, execution_time_seconds, created_at) SELECT 'bronze_claims_dbt', current_timestamp(), NULL, (SELECT COUNT(*) FROM {{ this }}), 0, 'bronze_claims_dbt', '{{ invocation_id }}', 'SUCCESS', NULL, current_timestamp()"
    ]
) }}


SELECT * FROM delta.`/Volumes/insurance/landing/streaming/claims/`

