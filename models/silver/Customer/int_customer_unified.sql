-- =============================================================================
-- int_customer_unified
-- Layer  : Silver
-- Grain  : one row per customer (unified across Salesforce + SAP)
-- Sources: stg_sf_account           (bronze)
--          stg_sap_kna1             (bronze)
--          stg_sap_knb1             (bronze)
--          stg_sap_knvv             (bronze)
-- =============================================================================

{{
    config(
        materialized         = 'incremental',
        unique_key           = 'customer_hash_key',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns',

        post_hook            = """
            DELETE FROM {{ this }} AS tgt
            WHERE NOT EXISTS (
                SELECT 1
                FROM (
                    SELECT DISTINCT sap_customer_number as customer_key
                    FROM   {{ ref('stg_sf_account') }}
                    WHERE  sap_customer_number IS NOT NULL

                    UNION

                    SELECT DISTINCT customer_number  as customer_key
                    FROM   {{ ref('stg_sap_kna1') }}
                ) AS src
                WHERE src.customer_key = tgt.customer_key
            )
        """
    )
}}


-- ── SOURCE CTEs ───────────────────────────────────────────────────────────────
-- Filtered to changed records on incremental runs.
-- coalesce on watermarks handles the first incremental run safely.

with sf_account as (

    select * from {{ ref('stg_sf_account') }}

    {% if is_incremental() %}
        where meta_effective_from > (
            select coalesce(max(sf_meta_effective_from), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}

),

sap_kna1 as (

    select * from {{ ref('stg_sap_kna1') }}

    {% if is_incremental() %}
        where meta_effective_from > (
            select coalesce(max(sap_meta_effective_from), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}
),

sap_knb1 as (
    select * from {{ ref('stg_sap_knb1') }}

    {% if is_incremental() %}
        and meta_effective_from > (
            select coalesce(max(sap_meta_effective_from), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}

),

sap_knvv as (

    select * from {{ ref('stg_sap_knvv') }}

    {% if is_incremental() %}
        where meta_effective_from > (
            select coalesce(max(sap_meta_effective_from), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}

),


-- ── STEP 1: join SAP tables internally ───────────────────────────────────────

sap_customer as (

    select

        kna1.customer_number,
        kna1.customer_name,
        kna1.country_code,
        kna1.city_region,
        kna1.created_date,

        knb1.company_code,
        knb1.payment_terms,
        case knb1.payment_terms
            when 'NT30' then 30
            when 'NT60' then 60
            when 'NT90' then 90
            else null
        end as payment_due_days,
        knb1.credit_limit_usd,

        knvv.distribution_channel,
        case knvv.distribution_channel
            when '10' then 'Direct'
            when '20' then 'Channel'
            else 'Unknown'
        end as deal_type,
        knvv.sales_office,

        -- watermark: latest update across all three SAP tables
        greatest(
            kna1.meta_effective_from, knb1.meta_effective_from, knvv.meta_effective_from)
                                                 as sap_meta_effective_from

    from sap_kna1 as kna1

    left join sap_knb1 as knb1
        on kna1.customer_number = knb1.customer_number

    left join sap_knvv as knvv
        on kna1.customer_number = knvv.customer_number

),


-- ── STEP 2: cross-system join ─────────────────────────────────────────────────
-- Anchored on Salesforce — accounts not yet in SAP are preserved with nulls.

unified as (

    select

        -- ── surrogate key (integer — used for all joins) ──────────────────
        hash(
            coalesce(sap.customer_number, sf.account_id)
        )                                         as customer_hash_key,

        -- ── natural key (varchar — for human readability and debugging) ───
        coalesce(
            sap.customer_number,
            sf.account_id
        )                                         as customer_key,

        -- ── system identifiers ────────────────────────────────────────────
        sf.account_id,
        sf.sap_customer_number,
        sap.customer_number,

        -- ── identity ──────────────────────────────────────────────────────
        -- Salesforce name preferred — it's the business-facing record
        coalesce(sf.account_name, sap.customer_name)
                                                  as customer_name,
        sf.account_type,
        sf.industry,

        -- ── geography ─────────────────────────────────────────────────────
        coalesce(sf.billing_country, sap.country_code)
                                                  as country_code,
        sf.billing_region                         as sales_region,            -- North America | EMEA | APAC
        sap.city_region,

        -- ── deal type ─────────────────────────────────────────────────────
        -- Keep both for DQ comparison — they should agree
        sf.channel_tier,
        sap.deal_type,

        -- Unified deal type: SF channel tier wins if set, else SAP, else Direct
        coalesce(
            case
                when sf.channel_tier is not null
                 and sf.channel_tier != ''
                then 'Channel'
            end,
            sap.deal_type,
            'Direct'
        )                                         as deal_type,

        -- ── financial attributes (SAP is source of truth) ─────────────────
        sap.payment_terms,
        sap.payment_due_days,
        sap.credit_limit_usd,
        sf.annual_revenue_usd,

        -- ── SAP org ───────────────────────────────────────────────────────
        sap.sales_office,
        sap.company_code,

        -- ── ownership ─────────────────────────────────────────────────────
        sf.owner_id,

        -- ── existence flags ───────────────────────────────────────────────
        sf.account_id is not null              as exists_in_salesforce,
        sap.customer_number is not null       as exists_in_sap,

        (sf.account_id is not null
            and sap.customer_number is null)  as is_salesforce_only,

        -- ── data quality: deal type mismatch across systems ───────────────
        (sf.channel_tier is not null
            and sf.channel_tier != ''
            and sap.deal_type = 'Direct')     as has_deal_type_mismatch,

        -- ── dates ─────────────────────────────────────────────────────────
        sf.created_date,
        sap.created_date,
        coalesce(sf.created_date, sap.created_date)
                                                  as customer_created_date,

        -- ── watermarks (read on next incremental run) ─────────────────────
        sf.meta_effective_from                    as sf_meta_effective_from,
        sap.sap_meta_effective_from,

        greatest(
            coalesce(sf.meta_effective_from,       '1900-01-01'::date),
            coalesce(sap.sap_meta_effective_from,  '1900-01-01'::date)
        )                                         as last_updated_date,

        current_timestamp                         as dbt_updated_at

    from sf_account as sf

    left join sap_customer as sap
        on sf.sap_customer_number = sap.customer_number

)

select * from unified