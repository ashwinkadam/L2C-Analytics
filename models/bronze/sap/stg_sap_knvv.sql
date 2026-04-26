-- =============================================================================
-- stg_sap__knvv
-- Grain  : one row per customer per sales area (org + channel + division)
-- Notes  : distribution_channel maps to deal_type in SF_OPPORTUNITY
--          10 = Direct, 20 = Channel — mirrors Salesforce Deal_Type__c
-- =============================================================================
with source as (

    select * from {{ source('sap', 'sap_knvv') }}
    where meta_is_current = 'true'

),

renamed as (

    select

        -- identifiers
        kunnr                                     as customer_number,
        vkorg                                     as sales_org,
        vtweg                                     as distribution_channel,   
        spart                                     as division,

        -- attributes
        kdgrp                                     as customer_group,
        waers                                     as currency_code,
        vkbur                                     as sales_office,           

        -- derived
        case vtweg
            when '10' then 'Direct'
            when '20' then 'Channel'
            else 'Unknown'
        end                                       as deal_type,

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