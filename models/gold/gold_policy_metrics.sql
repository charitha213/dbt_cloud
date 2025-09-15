{{
    config(
        materialized="table",
        schema="gold",
        post_hook=[
            "{{ log_model_run(source_relations=[ref('silver_policies_dbt'), ref('silver_customers_dbt')], bad_record_conditions=[], bad_record_source=ref('silver_policies_dbt')) }}"
        ],
    )
}}

with
    policies as (select * from {{ ref("silver_policies_dbt") }}),
    customers as (select * from {{ ref("silver_customers_dbt") }})

select
    p.policy_id,
    p.customer_id,
    c.first_name,
    c.last_name,
    p.policy_type,
    p.start_date as policy_start_date,
    p.end_date,
    p.premium_amount,
    case when p.premium_amount > 1000 then 'HIGH' else 'NORMAL' end as premium_category
from policies p
left join customers c on p.customer_id = c.customer_id
