{{ config(
    materialized='table',
    tags=['gold']
) }}

select * from {{ref('int_customer_unified')}}