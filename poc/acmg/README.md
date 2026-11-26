# ACMG classifier POC

Classifies the occurrences of the affected probands of a tenant (all of them, one import part, or some sequencings)
with the ACMG/AMP rules of `ruleset.yaml`, then ranks them by priority, in five steps:

| Step | Facts (`sql/`) | Grain | Output (in `--output-db`) |
|---|---|---|---|
| 1. variant | `variant_facts.sql` | (locus, MANE transcript) | `acmg_variant_criteria`: one row per criterion met |
| 2. occurrence | `occurrence_facts.sql` | (seq_id, locus), only where the case has evidence of its own | `acmg_occurrence_criteria`: one row per criterion met |
| 3. classification | `classification_facts.sql`: both criteria tables per occurrence | (seq_id, locus, MANE transcript) | `acmg_classification`: class, points, codes, cancelled criteria |
| 4. phenotype | `phenotype_facts.sql`: the case's observed HPO terms | (seq_id, gene) | `acmg_phenotype`: similarity, `top`, PP4 points |
| 5. priority | `priority.sql`, in StarRocks | (seq_id, locus, MANE transcript) | `acmg_priority`: ACMG points + PP4 + inheritance fit, as a probability |

Exclusions (BA1 cancels BS1, PVS1 cancels PP3, BP2 cancels PM3) and point caps (PM1 + PP3) apply at step 3, so the
criteria tables hold every criterion met, and `acmg_classification.acmg_cancelled` the ones dropped and by which
criterion. Each criterion row explains itself for a report: `value` and `threshold` (the main test), `source` (the
branch that met it: `revel` or `spliceai` for PP3, `clingen_hi` or `clinvar_lof` for PVS1, `gnomad` or `cohort` for
BA1/BS1/BS2), `details` (every other value it tested, e.g. PS4's 2x2 table and confidence bound) and `refs` (the
comphet partner's locus for PM3/BP2). The output tables are created if missing: drop them after a column change.

PP4 and the inheritance fit enter the priority only, never the ACMG class: PP4's strengths come from a simulation
(below), not from a clinical review, and never adds to a variant predicted harmless (`pp4.blocked_by`: BP4, BP7). The inheritance fit takes points from a HET variant of a recessive-only gene
(OMIM) with no comphet partner classed VUS or above (alone, it cannot explain the case), and from a variant of a
dominant-only gene carried by a non-affected parent (unlikely causal unless the penetrance is incomplete). Sort a case by
`priority_probability`, the probability of pathogenicity of the points (Tavtigian 2018: prior 0.1, 8 points = odds
350).

Steps 1 to 3 run the same way: StarRocks unloads the facts to Parquet on S3 (`INSERT INTO FILES()`), N worker processes
apply the rules (Polars expressions in `acmg/rules.py`, thresholds validated by the Pydantic models of
`acmg/models.py`), StarRocks loads their files back with one `INSERT ... SELECT FROM FILES()`. Before the load, the
rows of the scope are deleted: a rerun (new ClinVar, new occurrences) replaces them.

## Scope

- **Cases:** affected probands of `staging_sequencing_experiment` for `--tenant-code`; `--part n` or `--seq-ids` narrows them.
- **Classified:** the MANE Select consequence of the cases' loci, when it is one of `scope.consequences` or has
  SpliceAI ≥ `scope.spliceai_min`. Intergenic, deep intronic and UTR variants without splice signal are skipped.

## Criteria

| Level | Criterion | Data | Limit |
|---|---|---|---|
| variant | BA1, BS1, PM2_Supporting | `gnomad_genomes_v3` (global AF, AN) and the AF of the tenant's not affected (`snv__variant` carrier frequency, Hardy-Weinberg), the higher, except for ClinVar P/LP; PM2 dropped when common in the not affected | global AF, not grpmax; no BA1 exception list beyond ClinVar P/LP |
| variant | PP3, BP4 | REVEL (Pejaver 2022), SpliceAI (Walker 2023) | — |
| variant | BP7 | synonymous + low SpliceAI | — |
| variant | PVS1 | LoF consequence + ClinGen HI 3 or 30, or LOEUF + LoF P/LP in the gene (`clinvar.mc`), or many LoF P/LP whatever LOEUF, or a few in an OMIM recessive-only gene; never with HI 40; stop or frameshift in the last exon, or a canonical splice site whose skipped exon keeps the frame (`ensembl_exon_by_gene` phases) or is not found, gives `PVS1_Strong`; start lost `PVS1_Moderate` | no 50-nt rule, no critical region, no cryptic site |
| variant | PM4 | in-frame indel, stop lost | no repeat-region exception |
| variant | PM1 | ClinVar P/LP missense within ±25 bp, no B/LB missense; PM1 + PP3 capped at 4 points | POC thresholds |
| variant | PP2, BP1 | missense and LoF shares of the gene's ClinVar P/LP (`clinvar.mc`) | POC thresholds; `mc` format to check |
| variant | BS2 | homozygotes in gnomAD (Strong from 50, else Supporting) or the tenant's not-affected (Supporting), gene with an OMIM inheritance | POC thresholds; no penetrance data |
| variant | PS4 | odds ratio affected vs not affected of the tenant (`germline_pc/pn_{wgs,wxs}_{affected,not_affected}`), CI lower bound > 1, variant meeting PM2 | individuals, not families: relatives and the case itself counted; not affected are mostly parents |
| occurrence | PM6 | the slivar de novo call of the proband | all "assumed": one case = PM6_Supporting; no PS2 |
| occurrence | PP1, BS4 | the proband's affected parents carrying / not carrying the variant; BS4 in a dominant-only gene (OMIM) | PP1 never fires with 2 parents (< 3 meioses): segregation across families is for the ETL; BS4 assumes full penetrance |
| occurrence | PM3 | the proband's comphet partner (`germline__snv__comphet`) P/LP in ClinVar; "trans" in a complete trio | — |
| occurrence | BP2 | in trans with a ClinVar pathogenic variant in a dominant-only gene (OMIM) | no "in cis" branch |

OMIM inheritance (BS2, BS4, BP2, inheritance fit) is the classified transcript's gene's, from `omim_gene_panel`:
`snv__variant.omim_inheritance_code` is the variant's picked gene's, an overlapping gene's at some loci (KDM6B
variants carried AR, AD or nothing).

Every ClinVar read skips variants whose best RCV has fewer than `clinvar.min_stars` stars (`clinvar_rcv_summary`).
PVS1, PM1 and PP2 are dropped in a gene whose every ClinGen gene-disease validity is Disputed, Refuted or No Known
Disease Relationship (`clingen.no_disease_drops`).

Missing: PS1/PM5, PS2, PP4 in the class (priority only), PS3/BS3, BP3, BP5, PP5/BP6; PS4 from published cases (no literature source). Siblings: not read yet (probands only).

## Run locally

```sh
docker compose -f poc/acmg/docker-compose.yml up -d --wait    # StarRocks 4.0.16 + RustFS (S3)
cd poc/acmg
uv venv .venv --python 3.12 && uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python -m pytest tests
.venv/bin/python -m local.setup --n-loci 2000000 --n-cases 300 --variants-per-proband 2000
```

`local/` (the setup and its seed SQL) drops and refills synthetic stand-ins of the Radiant tables. It is not in the
`acmg` package, nor in the image (`.dockerignore`), and refuses any StarRocks but a local one and any database not
named `acmg_poc*`.

```sh
.venv/bin/python -m acmg.exchange [--part 3 | --seq-ids 9 12]
```

## ClinGen files

Each run rebuilds `clingen_gene` in `--output-db` from two ClinGen downloads (public, CC0), copied once under
`<S3_BUCKET>/clingen/`. Copy them again to take a newer ClinGen:

| File | Source |
|---|---|
| `ClinGen_gene_curation_list_GRCh38.tsv` | https://ftp.clinicalgenome.org/ClinGen_gene_curation_list_GRCh38.tsv |
| `gene-validity.csv` | https://search.clinicalgenome.org/kb/gene-validity/download |

## Gene symbols

Ensembl 114 (VEP), ClinVar, OMIM, ClinGen and HPO each name genes at their own release (SPATA5L1 is AFG2B since
2022, CENPJ is CPAP in ClinVar but not yet in Ensembl 114). Each run rebuilds `gene_symbol` in `--output-db` from
HGNC: every gene join goes through the current HGNC symbol, previous symbols included, aliases left out (genes share
them). Copy the file again under `<S3_BUCKET>/hgnc/` to take a newer HGNC:

| File | Source |
|---|---|
| `hgnc_complete_set.txt` | https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/hgnc_complete_set.txt |

## Phenotype score (PP4) simulation

`acmg.simulate` measures what a phenotype score would add to the ranking of a case's variants, without a clinical
truth: it spikes a ClinVar ≥ 2★ P/LP variant already classified into the classified occurrences of a random proband,
with a phenotype drawn from one of its gene's diseases (5 terms, 30 % made vaguer, 2 noise terms). The score is the
Resnik best-match average of the patient's terms to each gene's (`acmg/hpo.py`), expressed as `top`, the share of
annotated genes scoring at least as high. Half the simulations give the likelihood ratio of each `top` bin
(spikes vs background occurrences), turned into PP4 points (Tavtigian: Supporting = 350^(1/8)); the other half
give the spike's rank by ACMG points, by phenotype, and by both. It also counts the tenant's affected probands with
phenotypic HPO terms. Reads a classification run (`--output-db`) and the HPO files; writes nothing.

```sh
.venv/bin/python -m acmg.simulate --output-db acmg_poc_out [--clinical-db radiant_jdbc.public]
```

| File (under `<S3_BUCKET>/hpo/`) | Source |
|---|---|
| `hp.obo` | https://purl.obolibrary.org/obo/hp.obo |
| `genes_to_phenotype.txt` | https://github.com/obophenotype/human-phenotype-ontology/releases/latest/download/genes_to_phenotype.txt |

The simulation is optimistic: a disease's terms are the gene's own annotations, and the spike's gene-level ClinVar
criteria (PM1, PP2) count the spike itself (`ranks_without_clinvar_window_criteria` leaves those spikes out).

## Run on a real cluster

| | |
|---|---|
| Reads | the three facts queries: `<shared db>.snv__consequence`, `gnomad_genomes_v3`, `clinvar`, `clinvar_rcv_summary`, `ensembl_exon_by_gene`, `staging_sequencing_experiment`, `<tenant db>.snv__variant`, `germline__snv__occurrence`, `germline__snv__comphet`; the ClinGen files under `<S3_BUCKET>/clingen/`, the HGNC file under `<S3_BUCKET>/hgnc/` |
| Writes | the run's files under `s3://<bucket>/runs/<run id>/` (deleted at the end), the four tables of `--output-db` (created if missing; **refused if it is the shared or the tenant database**) |
| Never | DROP, TRUNCATE, UPDATE or DELETE on a Radiant table |
| Prints | counts per class and per criterion, file and row counts, timings |

Run it in a pod of the cluster (the workers read files derived from patient data), with `--shared-db`, `--tenant-db`,
`--tenant-code`, `--output-db`. Start with `--check` (EXPLAIN of the variant and occurrence facts queries: no row read,
nothing written), then a few `--seq-ids`, then every case.

Environment of the pod (values in the pod spec or a secret, never in this repo):

| Variable | Value |
|---|---|
| `STARROCKS_MYSQL_HOST`, `STARROCKS_MYSQL_PORT`, `STARROCKS_USER`, `STARROCKS_PASSWORD` | FE MySQL endpoint and user |
| `STARROCKS_SSL_CA` | CA file of the FE certificate, when TLS is on |
| `S3_USE_INSTANCE_PROFILE` | `true`: the workers use the pod's role, StarRocks its BEs' role |
| `S3_BUCKET`, `S3_REGION` | the exchange bucket, or `bucket/prefix` |

## Findings

Measured with ruleset `poc-11` on a production tenant: 348 affected probands, 10.2M classified occurrences, 76 s with 8
workers (peak 2.5 GB per worker, at the classification step).

| Class | Occurrences |
|---|---|
| Pathogenic | 16 |
| Likely pathogenic | 1,732 |
| VUS | 198,508 |
| Likely benign | 707,757 |
| Benign | 9,310,993 |

**Against ClinVar** (variants of the tenant with a ≥ 2-star P/LP or B/LB, no conflict; a variant takes its most
pathogenic class across its occurrences):

| ClinVar | Variants | Same side | VUS | Opposite side |
|---|---|---|---|---|
| P/LP | 531 | 47 % | 47 % | 6 % |
| B/LB | 57,249 | 92 % | 8 % | 13 variants |

- 95 % of the POC's P/LP with a ClinVar class are ClinVar P/LP.
- PVS1 in recessive genes doubled the P/LP found (24 % in `poc-9`): LOEUF measures heterozygous LoF intolerance, so
  it blocked PVS1 in genes whose carriers are healthy. Most of the new LP are single heterozygous LoF in recessive
  genes: right for the variant, not an explanation of the case on its own.
- The P/LP classed benign carry BS2 (≥ 50 gnomAD homozygotes) or BS1: pathogenic recessive variants that are common or
  incompletely penetrant. The BS2 and BP1 thresholds come from these measures (see the comments of `ruleset.yaml`).
- Still VUS: no functional (PS3) or literature evidence (PM3 from other families' cases, as Franklin does).
- Slightly optimistic: the gene-level ClinVar counts (PVS1, PP2, BP1) include the variant itself.

**Against Exomiser** (`poc-9`, its `acmg_classification`, same occurrences, candidates only): of 567 Exomiser P/LP,
the POC classes 25 % P/LP, 43 % VUS, 32 % B/LB. The B/LB are 20 LoF variants that Exomiser scores PVS1, which the POC
finds common (BA1, BS1, BS2; the BA1 ones in ~21 probands each, likely recurrent artefacts) or in a gene without a
known LoF mechanism.

**Priority** (`acmg.simulate`, ~265 test spikes per run; share of spikes in the top 10 of their case):

| Simulated phenotype | ACMG points | Phenotype | ACMG + PP4 | Median rank, ACMG + PP4 |
|---|---|---|---|---|
| 1 term | 43 % | 8 % | 74 % | 4.5 |
| 3 terms + 1 noise | 40 % | 31 % | 79 % | 4.5 |
| 5 terms + 2 noise | 35 % | 47 % | 83 % | 4 |

- PP4 mostly reorders VUS: probands with ≥ 6 priority points have a median of 3 such occurrences, with or without PP4
  (p90 6, max 10).
- Inheritance fit (`poc-12`, 3 terms + 1 noise): top 5 63 % → 77 % at −4 (69 % at −2, 82 % at −8), top 1 34 % → 41 %.
  −4 is kept: −8 gains little and costs more on a recessive diagnosis whose second allele is missed.
- PP4 blocked by BP4/BP7 (`poc-15`): same top 1 and top 5 as PP4 everywhere, top 10 88 % vs 87 %, top 50 97 % vs
  97 % (blocking every variant under 1 ACMG point instead: top 50 86 %); PP4 on 53k occurrences instead of 160k.
- With both inheritance rules and OMIM by gene (`poc-13`): −4 on 14 % of the occurrences and on 1,056 P/LP in 330
  probands (healthy carriers, dominant variants from a healthy parent); occurrences with ≥ 6 priority points per
  proband: median 5 → 2, p90 8 → 5, max 9; 42 probands have none (to review first: a missed second allele or an
  incompletely penetrant dominant variant would end there).
- 134 of the 348 probands have the inheritance root `HP:0000005` as their only "phenotype": no PP4 for them. With a
  phenotypic term, 202 probands, median 3 terms.
- A broad phenotype gives no PP4: a term shared by 40 % of the annotated genes (Seizure) ties them all.

**To make it a feature:**

- ETL: a classification table per tenant, refreshed with ClinVar and the occurrences; ClinGen dosage and gene validity
  loaded as open data (they are only IGV tracks today); gnomAD grpmax FAF for BA1/BS1 (the global AF is too coarse);
  gene-specific PM2/BS1 thresholds for recessive genes (maximum credible AF, Whiffin 2017: a pathogenic G6PC1
  frameshift at 0.017 % in gnomAD misses the flat 0.01 % PM2); the HPO ontology and gene annotations as open data.
- Portal: the class, points and criteria per occurrence, with each criterion's explanation (value, threshold, source, details, refs, cancellations); cases
  sorted by priority.
- Portal: a single HET variant of a recessive gene whose phenotype fits shown as "look for a second allele" (CNV,
  deep intronic), not only ranked down.
- Priority: the inheritance fit's cost, unmeasured (the simulation spikes causal variants that fit, so it misses a
  causal recessive variant whose second allele is not called: CNV, deep intronic, unclassified partner).
- Priority: PP4 is not reproducible. Two prod runs minutes apart, same data, move a few dozen occurrences between PP4
  points (4 points: 6641 then 6598), cause not found (an unstable tie in the similarity or `top` is the likely one).
- Clinical data: real phenotypes instead of `HP:0000005`.
- Missing criteria, by expected gain: PS1/PM5 (needs VEP-annotated ClinVar), PS2 (needs parentage).
- PVS1 decision tree (Abou Tayoun 2018), rest of it: 50-nt NMD rule, critical region, share of the protein removed,
  exon expression in the disease tissue, cryptic splice sites. Needs protein length, a critical-region source and
  per-exon expression. Done: exon-skipping frame from Ensembl phases, start lost `PVS1_Moderate`.
- A clinical review of the POC thresholds (PM1, PP2, BP1, BS2, PS4, PVS1's ClinVar LoF count) and of PP4's strengths.

### CNV (not started)

CNVs follow the ACMG/ClinGen CNV standard (Riggs 2020, Genet Med 22:245): points summed over 5 sections, with separate
tables for losses and gains (P ≥ 0.99, LP 0.90 to 0.98, B ≤ −0.99). Points to check against the paper's tables 1 and 2.

| Section | Evidence | Data |
|---|---|---|
| 1 | no gene nor functional element | `ensembl_gene` |
| 2 | overlap with dosage-sensitive genes or regions: ClinGen HI for losses, TS for gains; benign regions; ≥ 2 HI predictors | ClinGen gene and region files, gnomAD constraint |
| 2E (loss), 2I (gain) | both breakpoints in one gene: PVS1 on the deleted exons | reference transcript exons |
| 3 | number of protein-coding genes | `ensembl_gene` |
| 4O | common in the population | `gnomad_sf`, tenant not-affected frequency (`germline_cnv_variant_frequency`) |
| 5 | de novo, inherited from a non-affected parent | `transmission_mode`, parents' affected status |

Section 4 except 4O (literature, published cases) stays manual, at 0.

**Missing:**

- ClinGen TS: in the dosage TSV `clingen_gene` already reads; only `hi_score` is kept.
- ClinGen region file (recurrent syndromes, benign regions, with HI and TS).
- Reference transcript: `gene_reference_transcript` (MANE Select, else Ensembl canonical) is on ETL main, not in
  prod yet. Until then, rebuild it from `ensembl_transcript_v1` and `mane_v1` as `gene_reference_transcript_insert.sql`
  does, then filter `ensembl_exon_by_gene` on it.
- Call quality floor before classifying (`filter = 'PASS'`, size, quality), stricter for WXS: a false 3 Mb call
  scores in sections 2 and 3.

**Validation:** ClinVar CNVs from `variant_summary.txt` (copy number loss or gain, deletion or duplication ≥ 1 kb, ≥
1★: CNVs are mostly single-lab). Matched on the same type at 80 % reciprocal overlap, the rule of the gnomAD-SV and
cohort frequencies; count the matches at 50 % too, to measure what the array breakpoints cost. Several ClinVar records
for one CNV: majority class, conflicts counted apart. ClinVar stays out of the points, so the concordance does not
measure ClinVar against itself.

CNV + SNV comphet is a separate story, planned.

### Earlier POCs

POC 1 (rules in StarRocks SQL) and POC 2 (Arrow Flight read, Stream Load write; Python and Rust) were dropped. At 50M
synthetic loci, this exchange through Parquet loaded fastest (one `FILES()` load in one transaction beats Stream
Loads), and keeps the rules in Python. A list of structs is ~15x slower to build in Polars than one struct column per
criterion; rules row by row in plain Python with Pydantic objects were ~38x slower than Polars.
