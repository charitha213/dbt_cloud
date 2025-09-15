{{
    config(
        materialized="table",
        schema="silver",
        post_hook=[
            "{{ log_model_run(
    source_relation=ref('bronze_claims_dbt'),
    bad_record_conditions=[
        'claim_id is null',
        'policy_id is null',
        'claim_date is null',
        'incident_date > claim_date'
    ],
    bad_record_source=ref('bronze_claims_dbt') 
) }}
"
        ]
    )
}}

-- Cleaned and validated claims data
with
    renamed as (
        select
            claim_id,
            policy_id,
            adjuster_id,
            claim_amount,
            approved_amount,
            claim_date,
            incident_date,
            last_update_time,
            claim_number,
            claim_type,
            description,
            status
        from {{ ref("bronze_claims_dbt") }}
    ),

    deduped as (
        select
            *,
            row_number() over (
                partition by claim_id order by last_update_time desc
            ) as rn
        from renamed
    ),

    cleaned as (
        select *
        from deduped
        where
            rn = 1
            and claim_id is not null
            and policy_id is not null
            and claim_date is not null
            and incident_date <= claim_date
    )

select
    claim_id,
    policy_id,
    adjuster_id,
    round(claim_amount, 2) as claim_amount,
    round(approved_amount, 2) as approved_amount,
    claim_date,
    incident_date,
    last_update_time,
    upper(claim_number) as claim_number,
    initcap(claim_type) as claim_type,
    status,
from cleaned
