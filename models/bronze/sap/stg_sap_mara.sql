select 
    matnr as material_number,
    mtart as material_type,
    matkl as material_group,
    meins as unit_of_measure,
    erdat as creation_date,
    meta_src,
    meta_load_dt,
    meta_effective_from,
    meta_effective_to,
    meta_is_current
from
    {{source('sap','sap_mara')}}
where meta_is_current = true