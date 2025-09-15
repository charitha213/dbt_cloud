{{
    config(
        materialized="table",
        schema="silver",
        post_hook=[
            "{{ log_model_run(
                source_relation=ref('bronze_customers_dbt'),
                bad_record_conditions=[
                    'customer_id is null',
                    'email is null',
                    'length(phone) < 10',
                    'credit_score < 300 or credit_score > 850'
                ],
                bad_record_source=ref('bronze_customers_dbt')
            ) }}"
        ],
    )
}}

-- Cleaned and validated customers data
with
    renamed as (
        select
            customer_id,
            first_name,
            last_name,
            gender,
            date_of_birth,
            email,
            phone,
            address,
            city,
            state,
            zip_code,
            marital_status,
            employment_status,
            annual_income,
            credit_score,
            registration_date,
            last_update_time
        from {{ ref("bronze_customers_dbt") }}
    ),

    deduped as (
        select
            *,
            row_number() over (
                partition by customer_id order by last_update_time desc
            ) as rn
        from renamed
    ),

    cleaned as (
        select *
        from deduped
        where
            rn = 1
            and customer_id is not null
            and email is not null
            and length(phone) >= 10
            and credit_score between 300 and 850
    )

select
    customer_id,
    initcap(first_name) as first_name,
    initcap(last_name) as last_name,
    lower(gender) as gender,
    date_of_birth,
    lower(email) as email,
    phone,
    initcap(address) as address,
    initcap(city) as city,
    upper(state) as state,
    zip_code,
    lower(marital_status) as marital_status,
    lower(employment_status) as employment_status,
    round(annual_income, 2) as annual_income,
    credit_score,
    registration_date,
    last_update_time,
    datediff(current_date, date_of_birth) / 365 as age
from cleaned
