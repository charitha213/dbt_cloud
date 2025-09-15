{{
    config(
        materialized="table",
        schema="bronze",
        post_hook=[
            "{{ log_model_run(source_relation='delta.`/Volumes/insurance/landing/streaming/premiums/`') }}"
        ],
    )
}}

select *
from delta.`/Volumes/insurance/landing/streaming/premiums/`
