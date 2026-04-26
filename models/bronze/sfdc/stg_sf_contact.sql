-- =============================================================================
-- stg_sf_contact
-- Grain  : one row per Salesforce contact (current version)
-- Notes  : created at lead conversion — one contact per account by default
-- =============================================================================
with source as (

    select * from {{ source('sfdc', 'sf_contact') }}
    where meta_is_current = 'true'

),

renamed as (

    select

        -- identifiers
        id                                        as contact_id,
        accountid                                 as account_id,         
        ownerid                                   as owner_id,            

        -- attributes
        firstname                                 as first_name,
        lastname                                  as last_name,
        title                                     as contact_title,
        email                                     as email_address,
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