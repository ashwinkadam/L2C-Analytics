-- =============================================================================
--  macros/generate_schema_name.sql
--
--  Overrides dbt's default schema naming behaviour.
--
--  Default dbt behaviour appends the custom schema to the target schema:
--    target.schema = "bronze"  +  custom_schema = "bronze"
--    → produces "bronze_bronze"  ✗
--
--  This macro produces clean schema names:
--    custom_schema defined  →  use custom_schema exactly  (bronze | silver | gold)
--    no custom_schema       →  fall back to target.schema
--
--  Result per environment:
--    L2C_DEV.BRONZE   / L2C_DEV.SILVER   / L2C_DEV.GOLD
--    L2C_TEST.BRONZE  / L2C_TEST.SILVER  / L2C_TEST.GOLD
--    L2CPROD.BRONZE  / L2C_PROD.SILVER  / L2C_PROD.GOLD
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}

{%- endmacro %}