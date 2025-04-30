insert into {{ params.starrocks_variants_lookup }}(`hash`)
select `hash`
from {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.{{ params.iceberg_clinvar }} v
LEFT ANTI JOIN {{ params.starrocks_variants_lookup }} vd on vd.hash=v.hash;