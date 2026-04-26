
select 
    kunnr as customer_number,
    name1 as customer_name,
    land1 as country_code,
    ort01 as city,
    kukla as customer_classification,
    ktokd as account_group,
    stcd1 as tax_number,
    erdat::date as creation_date,
    meta_key,
    meta_src,
    meta_load_dt::timestamp_ntz as meta_load_dt,
    meta_effective_from::date as meta_effective_from,
    meta_effective_to::date as meta_effective_to
from
    {{source('sap','sap_kna1')}}
where 
    meta_is_current = true

    