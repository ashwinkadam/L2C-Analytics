-- stg_sap_makt
select 
    matnr as material_number,
    spras as lang,
    maktx as material_description,
    meta_key,
    meta_src,
    meta_load_dt::timestamp_ntz as meta_load_dt,
    meta_effective_from::date as meta_effective_from,
    meta_effective_to::date as meta_effective_to
from
    {{source('sap','sap_makt')}}
where 
    meta_is_current = true

    