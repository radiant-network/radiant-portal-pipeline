-- Synthetic cohort of tenant ${tenant_code}: ${n_cases} cases of one family each. Random, not realistic.
-- Case c: proband seq 3c (affected), mother 3c + 1 and father 3c + 2, one parent in ten affected.
-- One case in five is a solo (no parents).
INSERT INTO ${shared_db}.staging_sequencing_experiment
SELECT
    c,
    c * 3 + m,
    c * 3 + m,
    'germline',
    concat('P', c * 3 + m),
    'wgs',
    '${tenant_code}',
    c,
    CASE m WHEN 0 THEN 'proband' WHEN 1 THEN 'mother' ELSE 'father' END,
    IF(m = 0 OR abs(murmur_hash3_32(concat('aff', c, '-', m))) % 10 = 0, 'affected', 'non_affected'),
    FALSE
FROM TABLE(generate_series(1, ${n_cases})) AS g(c)
CROSS JOIN TABLE(generate_series(0, 2)) AS h(m)
WHERE m = 0 OR c % 5 <> 0;

-- ${variants_per_proband} variants per proband; one in five comes from a pool of 50,000 recurrent loci, so that
-- loci are shared by unrelated probands (PS4). In trios: 45 % maternal, 45 % paternal, 3 % de novo (slivar),
-- the rest with no-call parents. Only the probands' occurrences: the facts read no other.
INSERT INTO ${tenant_db}.germline__snv__occurrence
SELECT
    c % 10,
    c * 3,
    c * 3,
    locus,
    IF(q < 95, 20 + q % 60, 10),
    IF(q < 97, 3 + q % 30, 1),
    'HET',
    IF(q < 95, 'PASS', 'LowQual'),
    IF(trio, CASE origin WHEN 'father' THEN [0, 1] WHEN 'unknown' THEN [-1, -1] ELSE [0, 0] END, NULL),
    IF(trio, CASE origin WHEN 'mother' THEN [0, 1] WHEN 'unknown' THEN [-1, -1] ELSE [0, 0] END, NULL),
    IF(trio AND origin = 'denovo', 'autosomal_dominant_de_novo', NULL),
    IF(trio AND origin = 'denovo', 'slivar', NULL)
FROM (
    SELECT
        c,
        c % 5 <> 0 AS trio,
        IF(abs(murmur_hash3_32(concat('pool', c, '-', i))) % 5 = 0,
            1 + abs(murmur_hash3_32(concat('loc', c, '-', i))) % 50000,
            1 + abs(murmur_hash3_32(concat('loc', c, '-', i))) % ${n_loci}) AS locus,
        CASE
            WHEN o < 45 THEN 'mother'
            WHEN o < 90 THEN 'father'
            WHEN o < 93 THEN 'denovo'
            ELSE 'unknown'
        END AS origin,
        q
    FROM (
        SELECT
            c,
            i,
            abs(murmur_hash3_32(concat('org', c, '-', i))) % 100 AS o,
            abs(murmur_hash3_32(concat('qual', c, '-', i))) % 100 AS q
        FROM TABLE(generate_series(1, ${n_cases})) AS g(c)
        CROSS JOIN TABLE(generate_series(1, ${variants_per_proband})) AS h(i)
    ) draws
) x;

-- Best RCV stars per ClinVar locus: 20 % none, 55 % one, 20 % two, 5 % three; the hotspots two.
INSERT INTO ${shared_db}.clinvar_rcv_summary
SELECT
    locus_id,
    CASE
        WHEN locus_id >= 20000000000 THEN 2
        WHEN r < 20 THEN 0
        WHEN r < 75 THEN 1
        WHEN r < 95 THEN 2
        ELSE 3
    END
FROM (
    SELECT DISTINCT locus_id, abs(murmur_hash3_32(concat('stars', locus_id))) % 100 AS r
    FROM ${shared_db}.clinvar
) c;

-- Comphet pairs: one proband occurrence in 100 gets a partner drawn among the patient loci that are P/LP in ClinVar.
INSERT INTO ${tenant_db}.germline__snv__comphet
WITH plp AS (
    SELECT locus_id, row_number() OVER (ORDER BY locus_id) - 1 AS k
    FROM (
        SELECT DISTINCT locus_id
        FROM ${shared_db}.clinvar
        WHERE locus_id < 10000000000 AND arrays_overlap(clin_sig, ['Pathogenic', 'Likely_pathogenic'])
    ) c
),
total AS (SELECT count(*) AS n FROM plp),
pairs AS (
    SELECT o.part, o.seq_id, o.task_id, o.locus_id, o.draw % total.n AS k
    FROM (
        SELECT part, seq_id, task_id, locus_id, abs(murmur_hash3_32(concat('pair', seq_id, '-', locus_id))) AS draw
        FROM ${tenant_db}.germline__snv__occurrence
    ) o
    CROSS JOIN total
    WHERE o.draw % 100 = 0
)
SELECT p.part, p.seq_id, p.task_id, p.locus_id, plp.locus_id, 'GENE'
FROM pairs p
JOIN plp ON plp.k = p.k;

-- Affected carriers per locus, as the ETL's frequency step writes them (all counted as WGS here).
UPDATE ${tenant_db}.snv__variant
SET germline_pc_wgs_affected = a.pc, germline_pn_wgs_affected = a.pn
FROM (
    SELECT o.locus_id, count(DISTINCT s.patient_id) AS pc, ANY_VALUE(t.pn) AS pn
    FROM ${tenant_db}.germline__snv__occurrence o
    JOIN ${shared_db}.staging_sequencing_experiment s ON s.seq_id = o.seq_id
    CROSS JOIN (
        SELECT count(DISTINCT patient_id) AS pn FROM ${shared_db}.staging_sequencing_experiment
        WHERE affected_status = 'affected'
    ) t
    WHERE s.affected_status = 'affected' AND o.gq >= 20 AND o.filter = 'PASS' AND o.ad_alt >= 3
    GROUP BY o.locus_id
) a
WHERE snv__variant.locus_id = a.locus_id;

-- 1 to 3 HPO terms per proband, one in ten excluded (negative).
INSERT INTO ${shared_db}.obs_categorical
SELECT
    s.case_id,
    s.patient_id,
    'phenotype',
    'HPO',
    element_at(['HP:0001250', 'HP:0001263', 'HP:0000252', 'HP:0001252', 'HP:0004322', 'HP:0000486'],
               1 + abs(murmur_hash3_32(concat('hpo', s.seq_id, '-', t))) % 6),
    IF(abs(murmur_hash3_32(concat('neg', s.seq_id, '-', t))) % 10 = 0, 'negative', 'positive')
FROM ${shared_db}.staging_sequencing_experiment s
CROSS JOIN TABLE(generate_series(1, 3)) AS g(t)
WHERE s.family_role = 'proband' AND t <= 1 + abs(murmur_hash3_32(concat('n', s.seq_id))) % 3;

-- OMIM panel per gene, from the inheritance the seed gave the gene's variants.
INSERT INTO ${shared_db}.omim_gene_panel
SELECT DISTINCT k.symbol, concat('panel ', k.symbol), v.omim_inheritance_code
FROM ${tenant_db}.snv__variant v
JOIN ${shared_db}.snv__consequence k ON k.locus_id = v.locus_id
WHERE v.omim_inheritance_code IS NOT NULL AND array_length(v.omim_inheritance_code) > 0;
