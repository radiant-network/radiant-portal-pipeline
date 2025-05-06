insert into {{ params.starrocks_variants_lookup }}(`locus_hash`)
select `locus_hash`
from {{ params.iceberg_clinvar }} v
LEFT ANTI JOIN {{ params.starrocks_variants_lookup }} vd on vd.locus_hash=v.locus_hash;