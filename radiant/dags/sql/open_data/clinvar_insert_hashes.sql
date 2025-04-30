insert into variant_dict(`hash`)
select `hash`
from {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.clinvar v
LEFT ANTI JOIN variant_dict vd on vd.hash=v.hash;