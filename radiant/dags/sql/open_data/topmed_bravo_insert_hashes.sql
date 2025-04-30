INSERT INTO variant_dict(`hash`)
select
    `hash`
FROM {{ params.iceberg_catalog }}.{{ params.iceberg_database }}.topmed_bravo v
LEFT ANTI JOIN variant_dict vd ON vd.hash=v.hash;