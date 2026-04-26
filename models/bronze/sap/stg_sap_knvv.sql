select 
    kunnr as customer_number,
    vkorg as sales_org,
    vtweg as dist_channel,
    spart as division,
    kdgrp as customer_group,
    waers as currency,
    vkbur as sales_office,
    meta_key,
    meta_src,
    meta_load_dt::timestamp_ntz as meta_load_dt,
    meta_effective_from::date as meta_effective_from,
    meta_effective_to::date as meta_effective_to
from
    {{source('sap','sap_knvv')}}
where 
    meta_is_current = true
