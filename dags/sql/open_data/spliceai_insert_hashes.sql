INSERT INTO variant_dict(`hash`)
SELECT
    `hash`
FROM iceberg.poc_starrocks.spliceai_enriched s
LEFT ANTI JOIN variant_dict vd ON vd.hash=s.hash;