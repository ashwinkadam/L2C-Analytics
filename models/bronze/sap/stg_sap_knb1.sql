-- =============================================================================
-- stg_sap_knb1
-- Layer  : Bronze
-- Grain  : one row per customer per company code (current version only)
-- =============================================================================

with source as (

    select * from {{ source('sap', 'sap_knb1') }}
    where meta_is_current = 'true'
    and bukrs = 1000

),

renamed as (

    select

        -- ── identifiers ───────────────────────────────────────────────────
        kunnr                                     as customer_number,
        bukrs                                     as company_code,        -- 1000 

        -- ── payment and credit ────────────────────────────────────────────
        zterm                                     as payment_terms,       -- NT30 | NT60 | NT90
        klimk::number(18, 2)                      as credit_limit_usd,

        -- ── meta ──────────────────────────────────────────────────────────
        meta_key,
        meta_src,
        meta_load_dt::timestamp_ntz               as meta_load_dt,
        meta_effective_from::date                 as meta_effective_from,
        meta_effective_to::date                   as meta_effective_to,
        meta_is_current::boolean                  as meta_is_current

    from source

)

select * from renamed