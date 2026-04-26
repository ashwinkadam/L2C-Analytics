-- =============================================================================
-- stg_sap__mara
-- Grain  : one row per material number
-- Notes  : material_number maps to SF_PRODUCT2.product_code (cross-system)
-- =============================================================================
with source as (

    select * from {{ source('sap', 'sap_mara') }}
    where meta_is_current = 'true'

),

renamed as (

    select

    -- identifiers
    matnr                                     as material_number,     -- joins to SF_PRODUCT2.product_code

    -- attributes
    mtart                                     as material_type,       
    matkl                                     as material_group,      
    meins                                     as base_unit_of_measure,
    erdat::date                               as created_date,

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