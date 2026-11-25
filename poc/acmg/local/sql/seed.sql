-- Synthetic loci ${first} to ${last}, 3 transcripts each (MANE, other coding, non-coding). Random, not realistic.
-- Every draw is a hash of n, not rand(): StarRocks inlines subqueries and would draw rand() again at each use.

-- Locus n sits on chromosome 1 + n % 22, 50 bp from its neighbours on that chromosome.
-- Gene g = n % 20000 has an OMIM inheritance: g % 4 = 0 AR, 1 AD, 2 AD + AR, 3 none.
INSERT INTO ${tenant_db}.snv__variant
SELECT
    n,
    CAST(1 + n % 22 AS STRING),
    (n DIV 22) * 50 + 1,
    IF(r1 < 0.5, 40 + CAST(r2 * 200 AS INT), 5),
    IF(r3 < 0.1, r4 * 0.05, 0),
    IF(r5 < 0.5, 40 + CAST(r6 * 200 AS INT), 5),
    IF(r7 < 0.1, r8 * 0.05, 0),
    IF(r9 < 0.02, 1 + CAST(r2 * 3 AS INT), 0),
    0,
    0,
    0,
    0,
    0,
    IF(r3 < 0.1, CAST(r4 * 0.05 * IF(r1 < 0.5, 40 + CAST(r2 * 200 AS INT), 5) AS INT), 0),
    IF(r7 < 0.1, CAST(r8 * 0.05 * IF(r5 < 0.5, 40 + CAST(r6 * 200 AS INT), 5) AS INT), 0),
    CASE n % 20000 % 4 WHEN 0 THEN ['AR'] WHEN 1 THEN ['AD'] WHEN 2 THEN ['AD', 'AR'] END
FROM (
    SELECT
        n,
        abs(murmur_hash3_32(concat('v1', n))) % 10000 / 10000 AS r1,
        abs(murmur_hash3_32(concat('v2', n))) % 10000 / 10000 AS r2,
        abs(murmur_hash3_32(concat('v3', n))) % 10000 / 10000 AS r3,
        abs(murmur_hash3_32(concat('v4', n))) % 10000 / 10000 AS r4,
        abs(murmur_hash3_32(concat('v5', n))) % 10000 / 10000 AS r5,
        abs(murmur_hash3_32(concat('v6', n))) % 10000 / 10000 AS r6,
        abs(murmur_hash3_32(concat('v7', n))) % 10000 / 10000 AS r7,
        abs(murmur_hash3_32(concat('v8', n))) % 10000 / 10000 AS r8,
        abs(murmur_hash3_32(concat('v9', n))) % 10000 / 10000 AS r9
    FROM TABLE(generate_series(${first}, ${last})) AS s(n)
) x;

-- 70 % of loci are in gnomAD; homozygotes from Hardy-Weinberg (af^2 x an / 2).
INSERT INTO ${shared_db}.gnomad_genomes_v3
SELECT n, af, CAST(af * an AS INT), an, CAST(af * af * an / 2 AS INT)
FROM (
    SELECT
        n,
        pow(abs(murmur_hash3_32(concat('g1', n))) % 10000 / 10000, 6) * 0.2 AS af,
        IF(abs(murmur_hash3_32(concat('g2', n))) % 100 < 95, 152000, 1500) AS an
    FROM TABLE(generate_series(${first}, ${last})) AS s(n)
    WHERE abs(murmur_hash3_32(concat('g0', n))) % 100 < 70
) x;

INSERT INTO ${shared_db}.snv__consequence
SELECT
    n,
    concat('GENE', n % 20000),
    concat('ENST', n * 10 + t),
    [csq],
    IF(t = 3, 'retained_intron', 'protein_coding'),
    CAST(1 + rank_draw % exon_total AS STRING),
    CAST(exon_total AS STRING),
    pow(r_splice, 4),
    t = 1,
    t = 1,
    t = 1,
    IF(csq = 'missense_variant', r_revel, NULL),
    r_loeuf * 2
FROM (
    SELECT
        n,
        t,
        CASE
            WHEN r < 0.38 THEN 'missense_variant'
            WHEN r < 0.63 THEN 'synonymous_variant'
            WHEN r < 0.83 THEN 'intron_variant'
            WHEN r < 0.88 THEN 'stop_gained'
            WHEN r < 0.93 THEN 'frameshift_variant'
            WHEN r < 0.96 THEN 'splice_donor_variant'
            ELSE 'inframe_deletion'
        END AS csq,
        1 + abs(murmur_hash3_32(concat('c1', n, '-', t))) % 20 AS exon_total,
        abs(murmur_hash3_32(concat('c2', n, '-', t))) AS rank_draw,
        abs(murmur_hash3_32(concat('c3', n, '-', t))) % 10000 / 10000 AS r_splice,
        abs(murmur_hash3_32(concat('c4', n, '-', t))) % 10000 / 10000 AS r_revel,
        abs(murmur_hash3_32(concat('c5', n, '-', t))) % 10000 / 10000 AS r_loeuf
    FROM (
        SELECT n, t.t, abs(murmur_hash3_32(concat('c0', n))) % 10000 / 10000 AS r
        FROM TABLE(generate_series(${first}, ${last})) AS g(n)
        CROSS JOIN TABLE(generate_series(1, 3)) AS t(t)
    ) s
) x;

-- ClinVar background: 8.4 % of loci have an entry near them (30 % on the locus itself); with the hotspots, about
-- the 4.4M rows of the real table.
-- Gene g = n % 20000 has a mechanism: g % 3 = 0 LoF (BP1), 1 missense (PP2), 2 mixed.
INSERT INTO ${shared_db}.clinvar
SELECT
    IF(shift = 0, n, 10000000000 + n),
    CAST(1 + n % 22 AS STRING),
    (n DIV 22) * 50 + 1 + shift,
    CASE
        WHEN r1 < 0.125 THEN ['Pathogenic']
        WHEN r1 < 0.25 THEN ['Likely_pathogenic']
        WHEN r1 < 0.4 THEN ['Benign']
        WHEN r1 < 0.55 THEN ['Likely_benign']
        ELSE ['Uncertain_significance']
    END,
    [CASE
        WHEN r1 < 0.25 THEN CASE n % 20000 % 3
            WHEN 0 THEN IF(r2 < 0.9, 'SO:0001587|nonsense', 'SO:0001583|missense_variant')
            WHEN 1 THEN IF(r2 < 0.8, 'SO:0001583|missense_variant',
                IF(r2 < 0.9, 'SO:0001589|frameshift_variant', 'SO:0001627|intron_variant'))
            ELSE IF(r2 < 0.4, 'SO:0001583|missense_variant',
                IF(r2 < 0.8, 'SO:0001589|frameshift_variant', 'SO:0001819|synonymous_variant'))
        END
        WHEN r1 < 0.55 THEN IF(r2 < IF(n % 20000 % 3 = 1, 0.05, 0.4),
            'SO:0001583|missense_variant', 'SO:0001819|synonymous_variant')
        ELSE IF(r2 < 0.6, 'SO:0001583|missense_variant', 'SO:0001627|intron_variant')
    END],
    concat('GENE', n % 20000, ':', n % 20000)
FROM (
    -- Hashes of n, not rand(): StarRocks inlines this subquery and would draw rand() again at each use.
    SELECT
        n,
        abs(murmur_hash3_32(concat('r1', n))) % 10000 / 10000 AS r1,
        abs(murmur_hash3_32(concat('r2', n))) % 10000 / 10000 AS r2,
        IF(abs(murmur_hash3_32(concat('r3', n))) % 10 < 3, 0, abs(murmur_hash3_32(concat('r4', n))) % 48 - 24) AS shift
    FROM TABLE(generate_series(${first}, ${last})) AS g(n)
    WHERE abs(murmur_hash3_32(concat('r0', n))) % 1000 < 84
) x;

-- ClinVar hotspots: around one locus in 2,500, eight P/LP missense variants within 20 bp.
INSERT INTO ${shared_db}.clinvar
SELECT
    20000000000 + n * 8 + k,
    CAST(1 + n % 22 AS STRING),
    (n DIV 22) * 50 + 1 + k * 5 - 20,
    ['Pathogenic'],
    ['SO:0001583|missense_variant'],
    concat('GENE', n % 20000, ':', n % 20000)
FROM TABLE(generate_series(${first}, ${last})) AS g(n)
CROSS JOIN TABLE(generate_series(0, 7)) AS h(k)
WHERE n % 2500 = 0;
