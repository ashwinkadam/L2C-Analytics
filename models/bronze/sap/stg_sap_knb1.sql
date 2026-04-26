-- =============================================================================
-- stg_sap__knb1
-- Grain  : one row per customer per company code
-- Notes  : payment_terms and credit_limit are key for DSO and AR aging
-- =============================================================================
with source as (

    select * from {{ source('sap', 'sap_knb1') }}
    where meta_is_current = 'true'

),

renamed as (

    select

        -- identifiers
        kunnr                                     as customer_number,
        bukrs                                     as company_code,

        -- credit and payment
        zterm                                     as payment_terms,           
        klimk::number(18, 2)                      as credit_limit_usd,
        skfor::number(18, 2)                      as open_ar_balance_usd,
        nodel                                     as deletion_flag,

         -- derived
        case zterm
            when 'NT30' then 30
            when 'NT60' then 60
            when 'NT90' then 90
            else null
        end                                       as payment_due_days,

        -- meta columns (pass-through from ETL team)
        meta_key,
        meta_src,
        meta_load_dt::timestamp_ntz               as meta_load_dt,
        meta_effective_from::date                 as meta_effective_from,
        meta_effective_to::date                   as meta_effective_to,
        meta_is_current::boolean                  as meta_is_current

    from source

)

select * from renamed