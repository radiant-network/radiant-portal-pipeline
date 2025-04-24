INSERT INTO variant_dict(`hash`)
select
    `hash`
FROM iceberg.poc_starrocks.topmed_bravo v
LEFT ANTI JOIN variant_dict vd ON vd.hash=v.hash;