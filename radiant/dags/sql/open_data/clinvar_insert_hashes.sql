insert into {{ params.starrocks_variant_lookup }}(`locus_hash`)
select `locus_hash`
from {{ params.iceberg_clinvar }} v
LEFT ANTI JOIN {{ params.starrocks_variant_lookup }} vd on vd.locus_hash=v.locus_hash
WHERE GET_VARIANT_ID(v.chromosome, v.start, v.reference, v.alternate) IS NULL;