-- =============================================================================
-- stg_sf_account
-- Grain  : one row per Salesforce account (current version)
-- Notes  : sap_customer_number is the PRIMARY cross-system join key to KNA1
-- =============================================================================
with source as (

    select * from {{ source('sfdc', 'sf_account') }}
    where meta_is_current = 'true'

),

renamed as (

    select

        -- identifiers
        id                                        as account_id,
        ownerid                                   as owner_id,            
        sap_customer_number__c                    as sap_customer_number, 

        -- attributes
        name                                      as account_name,
        type                                      as account_type,        
        industry,
        annualrevenue::number(18, 2)              as annual_revenue_usd,
        billingcountry                            as billing_country,
        billingregion                             as billing_region,
        channel_tier__c                           as channel_tier,    
        createddate::date                         as created_date,

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