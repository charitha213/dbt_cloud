{{
    config(
        materialized="table",
        schema="gold",
        post_hook=[
            "{{ log_model_run( "
            "source_relations=[ref('silver_claims_dbt'), ref('silver_policies_dbt'), ref('silver_customers_dbt')], "
            "bad_record_conditions=[], "
            "bad_record_source=ref('silver_claims_dbt') "
            ") }}"
        ],
    )
}}

with
    claims as (select * from {{ ref("silver_claims_dbt") }}),
    customers as (select * from {{ ref("silver_customers_dbt") }}),
    policies as (select * from {{ ref("silver_policies_dbt") }}),

    enriched_claims as (
        select
            c.claim_id,
            c.policy_id,
            c.claim_amount,
            c.approved_amount,
            c.claim_date,
            p.customer_id,
            cu.first_name,
            cu.last_name,
            c.claim_severity
        from claims c
        left join policies p on c.policy_id = p.policy_id
        left join customers cu on p.customer_id = cu.customer_id
    )

select
    customer_id,
    first_name,
    last_name,
    count(distinct claim_id) as total_claims,
    sum(claim_amount) as total_claim_amount,
    sum(approved_amount) as total_approved_amount,
    sum(case when claim_severity = 'HIGH' then 1 else 0 end) as high_severity_claims
from enriched_claims
group by 1, 2, 3
