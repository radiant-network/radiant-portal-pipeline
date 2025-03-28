insert into kf_variants
SELECT
    v.locus_id,
    (vf.ac / 22000) as af,
    (vf.pc / 11000) as pf,
    g.af as gnomad_af,
    t.af as topmed_af,
    tg.af as tg_af,
    vf.ac as ac,
    vf.pc as pc,
    vf.hom as hom,
    v.chromosome,
    v.start,
    v.variant_class,
    cl.interpretations as clinvar_interpretation,
    v.symbol,
    v.consequence,
    v.vep_impact,
    v.mane_select,
    v.mane_plus,
    v.picked,
    v.canonical,
    v.rsnumber,
    v.reference,
    v.alternate,
    v.hgvsg,
    v.locus,
    v.dna_change,
    v.aa_change
from intermediate_kf_variants v
left join kf_variants_freq vf on vf.locus_id=v.locus_id
left join gnomad_genomes_v3 g on g.locus_id=v.locus_id
left join topmed_bravo t on t.locus_id=v.locus_id
left join 1000_genomes tg on tg.locus_id=v.locus_id
left join clinvar cl on cl.locus_id=v.locus_id;
