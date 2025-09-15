{{
    config(
        materialized="table",
        schema="silver",
        post_hook=[
            "{{ log_model_run(
                source_relation=ref('bronze_policies_dbt'),
                bad_record_conditions=[
                    'policy_id is null',
                    'customer_id is null',
                    'coverage_amount < 0',
                    'premium_amount < 0',
                    'end_date < start_date'
                ],
                bad_record_source=ref('bronze_policies_dbt')
            ) }}"
        ],
    )
}}

-- Cleaned and validated policies data
with
    renamed as (
        select
            policy_id,
            customer_id,
            policy_number,
            policy_type,
            coverage_amount,
            premium_amount,
            deductible,
            start_date,
            end_date,
            status,
            agent_id,
            last_update_time
        from {{ ref("bronze_policies_dbt") }}
    ),

    deduped as (
        select
            *,
            row_number() over (
                partition by policy_id order by last_update_time desc
            ) as rn
        from renamed
    ),

    cleaned as (
        select *
        from deduped
        where
            rn = 1
            and policy_id is not null
            and customer_id is not null
            and coverage_amount >= 0
            and premium_amount >= 0
            and end_date >= start_date
    )

select
    policy_id,
    customer_id,
    upper(policy_number) as policy_number,
    initcap(policy_type) as policy_type,
    round(coverage_amount, 2) as coverage_amount,
    round(premium_amount, 2) as premium_amount,
    round(deductible, 2) as deductible,
    start_date,
    end_date,
    lower(status) as status,
    agent_id,
    last_update_time,
    datediff(end_date, start_date) as policy_duration_days
from cleaned
