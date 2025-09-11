{{ config(materialized='table', schema='bronze') }}

SELECT *
FROM delta.`/Volumes/insurance/landing/streaming/customers/`