-- =============================================================================
-- stg_sap__knvv
-- Layer  : Bronze
-- Grain  : one row per customer per sales area 
-- =============================================================================

with source as (

    select * from {{ source('sap', 'sap_knvv') }}
    where meta_is_current = 'true'

),

renamed as (

    select

        -- ── identifiers ───────────────────────────────────────────────────
        kunnr                                     as customer_number,

        -- ── sales area ────────────────────────────────────────────────────
        vtweg                                     as distribution_channel, -- 10 = Direct | 20 = Channel
        vkbur                                     as sales_office,          -- NA01 | EU01 | AP01

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