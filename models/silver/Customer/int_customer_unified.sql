-- =============================================================================
-- int_customer_unified
-- Layer  : Silver
-- Grain  : one row per customer (unified across Salesforce + SAP)
-- Sources: stg_sf_account (bronze)
--          stg_sap_kna1             (bronze)
--          stg_sap_knb1             (bronze)
--          stg_sap_knvv             (bronze)
--
-- Incremental strategy: merge + post_hook DELETE
--
--   Execution order guaranteed:
--     1. MERGE  — upserts new and updated customers from bronze
--     2. DELETE — post_hook removes customers no longer in bronze
--                 (soft-deleted in source → meta_is_current = false
--                  → absent from stg_ bronze models → caught here)
--
--   Both steps run inside the same Snowflake transaction.
--   If the merge fails, the post_hook never fires.
--
-- Incremental filter applied at SOURCE CTEs (not at the bottom)
-- so unchanged customers never enter the join — no wasted compute.
--
-- Watermarks stored as columns (sf_meta_effective_from,
-- sap_meta_effective_from) so each system is tracked independently.
-- =============================================================================

{{
    config(
        materialized         = 'incremental',
        unique_key           = 'customer_key',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns',

        post_hook            = """
            DELETE FROM {{ this }} AS tgt
            WHERE NOT EXISTS (
                SELECT 1
                FROM (
                    SELECT DISTINCT sap_customer_number   AS customer_key
                    FROM   {{ ref('stg_sf_account') }}
                    WHERE  sap_customer_number IS NOT NULL

                    UNION

                    SELECT DISTINCT customer_number       AS customer_key
                    FROM   {{ ref('stg_sap_kna1') }}
                ) AS src
                WHERE src.customer_key = tgt.customer_key
            )
        """
    )
}}


-- ── SOURCE CTEs — filtered to changed records on incremental runs ─────────────
-- Filtering here means unchanged customers never enter the join.
-- coalesce on the watermark handles the very first incremental run
-- where {{ this }} exists but has no rows yet.

with sf_accounts as (

    select * from {{ ref('stg_sf_account') }}

    {% if is_incremental() %}
        where meta_effective_from::date > (
            select coalesce(max(sf_meta_effective_from), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}

),

sap_kna1 as (

    select * from {{ ref('stg_sap_kna1') }}

    {% if is_incremental() %}
        where meta_effective_from::date > (
            select coalesce(max(sap_meta_effective_from), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}

),

sap_knb1 as (

    select * from {{ ref('stg_sap_knb1') }}
    where company_code = '1000'

    {% if is_incremental() %}
        and meta_effective_from::date > (
            select coalesce(max(sap_meta_effective_from), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}

),

sap_knvv as (

    select * from {{ ref('stg_sap_knvv') }}
    where sales_org = '1000'

    {% if is_incremental() %}
        and meta_effective_from::date > (
            select coalesce(max(sap_meta_effective_from), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}

),


-- ── STEP 1: join SAP tables internally ───────────────────────────────────────
-- Resolve SAP customer master across three tables before the cross-system join.
-- coalesce on knb1/knvv dates guards against NULL from greatest()
-- when a customer has no KNB1 or KNVV row.

sap_customer_full as (

    select

        kna1.customer_number                      as sap_customer_number,
        kna1.customer_name                        as sap_customer_name,
        kna1.country_code                         as sap_country_code,
        kna1.city_region                          as sap_city_region,
        kna1.customer_classification              as sap_customer_classification,
        kna1.account_group_code                   as sap_account_group_code,
        kna1.customer_type                        as sap_customer_type,
        kna1.tax_number                           as sap_tax_number,
        kna1.created_date                         as sap_created_date,

        knb1.payment_terms                        as payment_terms,
        knb1.payment_due_days                     as payment_due_days,
        knb1.credit_limit_usd                     as credit_limit_usd,
        knb1.open_ar_balance_usd                  as open_ar_balance_usd,
        knb1.deletion_flag                        as sap_deletion_flag,

        knvv.distribution_channel                 as sap_distribution_channel,
        knvv.deal_type                            as sap_deal_type,
        knvv.sales_office                         as sap_sales_office,
        knvv.customer_group                       as sap_customer_group,
        knvv.currency_code                        as sap_currency_code,

        -- coalesce guards against NULL from greatest() when knb1/knvv are absent
        greatest(
            kna1.meta_effective_from,
            coalesce(knb1.meta_effective_from, kna1.meta_effective_from),
            coalesce(knvv.meta_effective_from, kna1.meta_effective_from)
        )                                         as sap_meta_effective_from

    from sap_kna1 as kna1
    left join sap_knb1 as knb1
        on  kna1.customer_number = knb1.customer_number
    left join sap_knvv as knvv
        on  kna1.customer_number = knvv.customer_number

),


-- ── STEP 2: cross-system join — Salesforce LEFT JOIN SAP ─────────────────────
-- Anchored on Salesforce so accounts not yet in SAP are preserved.

unified as (

    select

        -- ── surrogate key ─────────────────────────────────────────────────
        coalesce(
            sap.sap_customer_number,
            sf.account_id
        )                                         as customer_key,

        -- ── system identifiers ────────────────────────────────────────────
        sf.account_id                             as sf_account_id,
        sf.sap_customer_number                    as sf_sap_customer_number,
        sap.sap_customer_number                   as sap_customer_number,

        -- ── identity ──────────────────────────────────────────────────────
        coalesce(sf.account_name, sap.sap_customer_name)
                                                  as customer_name,
        sf.account_type                           as sf_account_type,
        sf.industry                               as industry,

        -- ── geography ─────────────────────────────────────────────────────
        coalesce(sf.billing_country, sap.sap_country_code)
                                                  as country_code,
        sf.billing_region                         as sales_region,
        sap.sap_city_region                       as city_region,

        -- ── deal type ─────────────────────────────────────────────────────
        sf.channel_tier                           as sf_channel_tier,
        sap.sap_deal_type                         as sap_deal_type,
        coalesce(
            case
                when sf.channel_tier is not null
                 and sf.channel_tier != ''
                then 'Channel'
            end,
            sap.sap_deal_type,
            'Direct'
        )                                         as deal_type,

        -- ── financials (SAP is source of truth) ───────────────────────────
        sap.payment_terms                         as payment_terms,
        sap.payment_due_days                      as payment_due_days,
        sap.credit_limit_usd                      as credit_limit_usd,
        sap.open_ar_balance_usd                   as open_ar_balance_usd,
        sf.annual_revenue_usd                     as annual_revenue_usd,

        -- ── SAP org ───────────────────────────────────────────────────────
        sap.sap_sales_office                      as sap_sales_office,
        sap.sap_customer_group                    as sap_customer_group,
        sap.sap_currency_code                     as sap_currency_code,
        sap.sap_deletion_flag                     as sap_deletion_flag,

        -- ── ownership ─────────────────────────────────────────────────────
        sf.owner_id                               as sf_owner_id,

        -- ── existence flags ───────────────────────────────────────────────
        case
            when sf.account_id is not null
            then true else false
        end                                       as exists_in_salesforce,

        case
            when sap.sap_customer_number is not null
            then true else false
        end                                       as exists_in_sap,

        case
            when sf.account_id is not null
             and sap.sap_customer_number is null
            then true else false
        end                                       as is_salesforce_only,

        -- ── data quality ──────────────────────────────────────────────────
        case
            when sf.channel_tier is not null
             and sf.channel_tier != ''
             and sap.sap_deal_type = 'Direct'
            then true else false
        end                                       as has_deal_type_mismatch,

        -- ── dates ─────────────────────────────────────────────────────────
        sf.created_date                           as sf_created_date,
        sap.sap_created_date                      as sap_created_date,
        coalesce(sf.created_date, sap.sap_created_date)
                                                  as customer_created_date,

        -- ── watermarks — read by source CTEs on next incremental run ──────
        sf.meta_effective_from                    as sf_meta_effective_from,
        sap.sap_meta_effective_from               as sap_meta_effective_from,

        greatest(
            coalesce(sf.meta_effective_from,      '1900-01-01'::date),
            coalesce(sap.sap_meta_effective_from, '1900-01-01'::date)
        )                                         as last_updated_date,

        current_timestamp                         as dbt_updated_at

    from sf_accounts as sf
    left join sap_customer_full as sap
        on sf.sap_customer_number = sap.sap_customer_number

)

select * from unified