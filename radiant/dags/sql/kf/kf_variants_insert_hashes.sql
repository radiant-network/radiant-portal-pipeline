INSERT INTO variant_dict(`hash`)
SELECT
    `hash`
FROM iceberg.poc_starrocks.kf_variants v
LEFT ANTI JOIN variant_dict vd ON vd.hash=v.hash;