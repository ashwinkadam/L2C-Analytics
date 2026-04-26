
select 
    kunnr as customer_number,
    bukrs as company_code,
    zterm as payment_terms,
    klimk as credit_limit,
    skfor as balance,
    nodel as deletion_flag,
    meta_key,
    meta_src,
    meta_load_dt::timestamp_ntz as meta_load_dt,
    meta_effective_from::date as meta_effective_from,
    meta_effective_to::date as meta_effective_to
from
    {{source('sap','sap_knb1')}}
where 
    meta_is_current = true
