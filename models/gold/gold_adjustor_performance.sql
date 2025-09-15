{{
    config(
        materialized="table",
        post_hook=[
            "{{ log_model_run(source_relation=ref('silver_claims_dbt'), bad_record_conditions=[], bad_record_source=ref('silver_claims_dbt')) }}"
        ]
    )
}}

with
    claims as (
        select * from {{ ref('silver_claims_dbt') }}
    )

select
    adjuster_id,
    count(distinct claim_id) as total_claims_handled,
    sum(claim_amount) as total_claim_amount,
    avg(approved_amount) as avg_approved_amount,
    sum(case when claim_severity = 'HIGH' then 1 else 0 end) as high_severity_claims
from claims
group by adjuster_id
