{{ config(
    materialized='table',
    schema='bronze'
) }}


SELECT * FROM delta.`/Volumes/insurance/landing/streaming/claims/`

-- at the bottom of the file
{{ log_bronze_run('bronze_claims_dbt') }}
