-- =============================================================================
-- stg_sap__kna1
-- Grain  : one row per SAP customer number
-- Notes  : customer_number maps to SF_ACCOUNT.sap_customer_number (cross-system)
--          account_group_code distinguishes direct (0001) vs channel (0002)
-- =============================================================================
with source as (

    select * from {{ source('sap', 'sap_kna1') }}
    where meta_is_current = 'true'

),

renamed as (

    select

        -- identifiers
        kunnr                                     as customer_number,     -- joins to SF_ACCOUNT.sap_customer_number

        -- attributes
        name1                                     as customer_name,
        land1                                     as country_code,
        ort01                                     as city_region,
        kukla                                     as customer_classification,
        ktokd                                     as account_group_code,  -- 0001 direct | 0002 channel
        stcd1                                     as tax_number,
        erdat::date                               as created_date,
        
        -- derived
        case ktokd
            when '0001' then 'Direct'
            when '0002' then 'Channel'
            else 'Unknown'
        end                                       as customer_type,


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