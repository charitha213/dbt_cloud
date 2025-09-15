{{
    config(
        materialized="table",
        schema="gold",
        post_hook=[
            "{{ log_model_run(source_relations=[ref('silver_claims_dbt'), ref('silver_policies_dbt'), ref('silver_customers_dbt')], bad_record_conditions=[], bad_record_source=ref('silver_claims_dbt')) }}"
        ]
    )
}}


with
    claims as (
        select * from {{ ref('silver_claims_dbt') }}
    ),
    policies as (
        select * from {{ ref('silver_policies_dbt') }}
    ),
    customers as (
        select * from {{ ref('silver_customers_dbt') }}
    )

select
    cu.customer_id,
    cu.first_name,
    cu.last_name,
    count(distinct p.policy_id) as total_policies,
    count(distinct cl.claim_id) as total_claims,
    sum(cl.claim_amount) as total_claim_amount,
    sum(cl.approved_amount) as total_approved_amount,
    case 
        when sum(cl.approved_amount) > 50000 then 'VIP'
        when sum(cl.approved_amount) > 10000 then 'LOYAL'
        else 'NEW'
    end as customer_segment
from customers cu
left join policies p on cu.customer_id = p.customer_id
left join claims cl on p.policy_id = cl.policy_id
group by cu.customer_id, cu.first_name, cu.last_name
