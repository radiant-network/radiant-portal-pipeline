insert into variant_dict(`hash`)
select `hash`
from iceberg.poc_starrocks.clinvar v
LEFT ANTI JOIN variant_dict vd on vd.hash=v.hash;