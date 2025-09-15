{{
    config(
        materialized="table",
        schema="silver",
        post_hook=[
            "{{ log_model_run(
                source_relation=ref('bronze_premiums_dbt'),
                bad_record_conditions=[
                    'premium_id is null',
                    'policy_id is null',
                    'amount_due < 0',
                    'amount_paid < 0',
                    'due_date is null'
                ],
                bad_record_source=ref('bronze_premiums_dbt')
            ) }}"
        ],
    )
}}

-- Cleaned and validated premiums data
with
    renamed as (
        select
            premium_id,
            policy_id,
            amount_due,
            amount_paid,
            late_fee,
            due_date,
            payment_date,
            payment_method,
            status,
            last_update_time
        from {{ ref("bronze_premiums_dbt") }}
    ),

    deduped as (
        select
            *,
            row_number() over (
                partition by premium_id order by last_update_time desc
            ) as rn
        from renamed
    ),

    cleaned as (
        select *
        from deduped
        where
            rn = 1
            and premium_id is not null
            and policy_id is not null
            and amount_due >= 0
            and amount_paid >= 0
            and due_date is not null
    )

select
    premium_id,
    policy_id,
    round(amount_due, 2) as amount_due,
    round(amount_paid, 2) as amount_paid,
    round(late_fee, 2) as late_fee,
    due_date,
    payment_date,
    lower(payment_method) as payment_method,
    lower(status) as status,
    last_update_time,
    case
        when amount_paid >= amount_due then 'PAID'
        when amount_paid < amount_due and due_date < current_date then 'OVERDUE'
        else 'PENDING'
    end as payment_status_flag
from cleaned
