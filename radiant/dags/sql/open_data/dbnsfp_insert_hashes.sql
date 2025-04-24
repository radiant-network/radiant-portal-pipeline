INSERT INTO variant_dict(`hash`)
SELECT
    `hash`
FROM iceberg.poc_starrocks.dbnsfp d
LEFT ANTI JOIN variant_dict vd ON vd.hash=d.hash;