{{ config(
    materialized="table",
    schema="bronze",
    post_hook=[
        "{{ log_model_run(source_relation='delta.`/Volumes/insurance/landing/streaming/customers/`') }}"
    ]
) }}

SELECT *
FROM delta.`/Volumes/insurance/landing/streaming/customers/`
