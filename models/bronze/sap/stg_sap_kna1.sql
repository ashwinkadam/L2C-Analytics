-- =============================================================================
-- stg_sap_kna1
-- Layer  : Bronze
-- Grain  : one row per SAP customer number 
-- =============================================================================

with source as (

    select * from {{ source('sap', 'sap_kna1') }}
    where meta_is_current = 'true'

),

renamed as (

    select

        -- ── identifiers ───────────────────────────────────────────────────
        kunnr                                     as customer_number,     -- → sf_sap_customer_number in SF_ACCOUNT

        -- ── attributes ───────────────────────────────────────────────────
        name1                                     as customer_name,

        -- ── geography ─────────────────────────────────────────────────────
        land1                                     as country_code,        -- ISO 2-letter
        ort01                                     as city_region,

        -- ── dates ─────────────────────────────────────────────────────────
        erdat::date                               as created_date,

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