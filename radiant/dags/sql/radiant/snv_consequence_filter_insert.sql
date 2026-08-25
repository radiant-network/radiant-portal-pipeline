INSERT OVERWRITE {{ mapping.starrocks_snv_consequence_filter }}
SELECT
    t.locus_id,
    NOT (sift_score IS NULL AND fathmm_score IS NULL AND polyphen2_hvar_score IS NULL AND cadd_score IS NULL AND dann_score IS NULL AND lrt_score IS NULL AND revel_score IS NULL AND phyloP17way_primate IS NULL AND phyloP100way_vertebrate IS NULL AND spliceai_ds IS NULL) AS is_deleterious,
    impact_score,
    symbol,
    consequence,
    biotype,
    spliceai_ds,
    sift_score,
    sift_pred,
    polyphen2_hvar_score,
    polyphen2_hvar_pred,
    fathmm_score,
    fathmm_pred,
    cadd_score,
    cadd_phred,
    dann_score,
    revel_score,
    lrt_score,
    lrt_pred,
    gnomad_pli,
    gnomad_loeuf,
    phyloP17way_primate,
    phyloP100way_vertebrate,
    vep_impact
FROM (
    SELECT
        locus_id,
        impact_score,
        symbol,
        consequence,
        biotype,
        spliceai_ds,
        sift_score,
        ANY_VALUE(sift_pred) AS sift_pred,
        polyphen2_hvar_score,
        ANY_VALUE(polyphen2_hvar_pred) AS polyphen2_hvar_pred,
        fathmm_score,
        ANY_VALUE(fathmm_pred) AS fathmm_pred,
        ANY_VALUE(cadd_score) AS cadd_score,
        ANY_VALUE(cadd_phred) AS cadd_phred,
        ANY_VALUE(dann_score) AS dann_score,
        revel_score,
        ANY_VALUE(lrt_score) AS lrt_score,
        ANY_VALUE(lrt_pred) AS lrt_pred,
        gnomad_pli,
        ANY_VALUE(gnomad_loeuf) AS gnomad_loeuf,
        ANY_VALUE(phyloP17way_primate) AS phyloP17way_primate,
        ANY_VALUE(phyloP100way_vertebrate) AS phyloP100way_vertebrate,
        vep_impact
    FROM (
        SELECT
            locus_id,
            impact_score,
            symbol,
            unnest as consequence,
            biotype,
            spliceai_ds,
            sift_score,
            sift_pred,
            polyphen2_hvar_score,
            polyphen2_hvar_pred,
            fathmm_score,
            fathmm_pred,
            cadd_score,
            cadd_phred,
            dann_score,
            revel_score,
            lrt_score,
            lrt_pred,
            gnomad_pli,
            gnomad_loeuf,
            phyloP17way_primate,
            phyloP100way_vertebrate,
            vep_impact,
            -- Not selected further up; it only feeds the anti join below.
            source
        FROM
            {{ mapping.starrocks_snv_consequence }} c,
            UNNEST(consequences) AS unnest
    ) gr
    -- SJRA-1828. RefSeq loads only where Ensembl is silent on that (locus, symbol, consequence). Why the
    -- source conjunct sits in the ON clause, and why vep_impact is not a key: section 7 of
    -- design/SJRA-1820-vep-merged-refseq-ingestion.md.
    LEFT ANTI JOIN (
        SELECT
            locus_id,
            symbol,
            -- StarRocks names the unnested column `unnest` whatever the table alias is.
            unnest AS consequence
        FROM
            {{ mapping.starrocks_snv_consequence }} e,
            UNNEST(consequences) AS unnest
        WHERE source = 'Ensembl'
    ) ens
        ON  ens.locus_id = gr.locus_id
        AND ens.symbol = gr.symbol
        AND ens.consequence = gr.consequence
        AND gr.source = 'RefSeq'
    GROUP BY
        locus_id,
        consequence,
        symbol,
        biotype,
        sift_score,
        polyphen2_hvar_score,
        fathmm_score,
        revel_score,
        spliceai_ds,
        impact_score,
        gnomad_pli,
        vep_impact
) t
