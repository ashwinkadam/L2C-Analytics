-- =============================================================================
-- stg_sap__makt
-- Grain  : one row per material per language
-- =============================================================================
with source as (

    select * from {{ source('sap', 'sap_makt') }}
    where meta_is_current = 'true'

),

renamed as (

    select

    -- identifiers
    matnr                                     as material_number,

    -- attributes
    spras                                     as language_key,
    maktx                                     as material_description,

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