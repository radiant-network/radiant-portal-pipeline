# ACMG classifier POC

Classifies the occurrences of the affected probands of a tenant (all of them, one import part, or some sequencings)
with the ACMG/AMP rules of `ruleset.yaml`, in three steps:

| Step | Facts (`sql/`) | Grain | Output (in `--output-db`) |
|---|---|---|---|
| 1. variant | `variant_facts.sql` | (locus, MANE transcript) | `acmg_variant_criteria`: one row per criterion met |
| 2. occurrence | `occurrence_facts.sql` | (seq_id, locus), only where the case has evidence of its own | `acmg_occurrence_criteria`: one row per criterion met |
| 3. classification | `classification_facts.sql`: both criteria tables per occurrence | (seq_id, locus, MANE transcript) | `acmg_classification`: class, points, codes |

Exclusions (BA1 cancels BS1, PVS1 cancels PP3, BP2 cancels PM3) and point caps (PM1 + PP3) apply at step 3, so the
criteria tables hold every criterion met, with its value and threshold.

Each step runs the same way: StarRocks unloads the facts to Parquet on S3 (`INSERT INTO FILES()`), N worker processes
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
| variant | PVS1 | LoF consequence + ClinGen HI 3 or 30, or LOEUF + LoF P/LP in the gene (`clinvar.mc`); never with HI 40; stop or frameshift in the last exon gives `PVS1_Strong` | no 50-nt rule, no critical region |
| variant | PM4 | in-frame indel, stop lost | no repeat-region exception |
| variant | PM1 | ClinVar P/LP missense within ±25 bp, no B/LB missense; PM1 + PP3 capped at 4 points | POC thresholds |
| variant | PP2, BP1 | missense and LoF shares of the gene's ClinVar P/LP (`clinvar.mc`) | POC thresholds; `mc` format to check |
| variant | BS2 | homozygotes in gnomAD (Strong from 50, else Supporting) or the tenant's not-affected (Supporting), gene with an OMIM inheritance | POC thresholds; no penetrance data |
| variant | PS4 | odds ratio affected vs not affected of the tenant (`germline_pc/pn_{wgs,wxs}_{affected,not_affected}`), CI lower bound > 1, variant meeting PM2 | individuals, not families: relatives and the case itself counted; not affected are mostly parents |
| occurrence | PM6 | the slivar de novo call of the proband | all "assumed": one case = PM6_Supporting; no PS2 |
| occurrence | PP1, BS4 | the proband's affected parents carrying / not carrying the variant; BS4 in a dominant-only gene (OMIM) | PP1 never fires with 2 parents (< 3 meioses): segregation across families is for the ETL; BS4 assumes full penetrance |
| occurrence | PM3 | the proband's comphet partner (`germline__snv__comphet`) P/LP in ClinVar; "trans" in a complete trio | — |
| occurrence | BP2 | in trans with a ClinVar pathogenic variant in a dominant-only gene (OMIM) | no "in cis" branch |

Every ClinVar read skips variants whose best RCV has fewer than `clinvar.min_stars` stars (`clinvar_rcv_summary`).
PVS1, PM1 and PP2 are dropped in a gene whose every ClinGen gene-disease validity is Disputed, Refuted or No Known
Disease Relationship (`clingen.no_disease_drops`).

Missing: PS1/PM5, PS2, PP4, PS3/BS3, BP3, BP5, PP5/BP6; PS4 from published cases (no literature source). Siblings: not read yet (probands only).

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

## Run on a real cluster

| | |
|---|---|
| Reads | the three facts queries: `<shared db>.snv__consequence`, `gnomad_genomes_v3`, `clinvar`, `clinvar_rcv_summary`, `staging_sequencing_experiment`, `<tenant db>.snv__variant`, `germline__snv__occurrence`, `germline__snv__comphet`; the ClinGen files under `<S3_BUCKET>/clingen/` |
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

Measured with ruleset `poc-9` on a production tenant: 696 affected probands, 10.2M classified occurrences, 61 s with 8
workers (peak 2.5 GB per worker, at the classification step).

| Class | Occurrences |
|---|---|
| Pathogenic | 10 |
| Likely pathogenic | 1,007 |
| VUS | 198,823 |
| Likely benign | 707,975 |
| Benign | 9,311,191 |

**Against ClinVar** (variants of the tenant with a ≥ 2-star P/LP or B/LB, no conflict):

| ClinVar | Variants | Same side | VUS | Opposite side |
|---|---|---|---|---|
| P/LP | 532 | 24 % | 68 % | 8 % |
| B/LB | 57,567 | 92 % | 8 % | 7 variants |

- P/LP mostly end as VUS: the classifier has no functional (PS3), phenotype (PP4) or literature evidence.
- The P/LP classed benign carry BS2 (≥ 50 gnomAD homozygotes) or BS1: pathogenic recessive variants that are common or
  incompletely penetrant. The BS2 and BP1 thresholds come from these measures (see the comments of `ruleset.yaml`).
- Slightly optimistic: the gene-level ClinVar counts (PVS1, PP2, BP1) include the variant itself.

**Against Exomiser** (its `acmg_classification`, same occurrences, candidates only): of 567 Exomiser P/LP, the POC
classes 25 % P/LP, 43 % VUS, 32 % B/LB. The B/LB are 20 LoF variants that Exomiser scores PVS1, which the POC finds
common (BA1, BS1, BS2; the BA1 ones in ~21 probands each, likely recurrent artefacts) or in a gene without a known
LoF mechanism.

**To make it a feature:**

- ETL: a classification table per tenant, refreshed with ClinVar and the occurrences; ClinGen dosage and gene validity
  loaded as open data (they are only IGV tracks today); gnomAD grpmax FAF for BA1/BS1 (the global AF is too coarse).
- Portal: the class, points and criteria per occurrence, with the audit of each criterion (value, threshold).
- Missing criteria, by expected gain: PP4 (Exomiser HPO, Biesecker 2024 strengths), PS1/PM5 (needs VEP-annotated
  ClinVar), PS2 (needs parentage).
- A clinical review of the POC thresholds (PM1, PP2, BP1, BS2, PS4).

### Earlier POCs

POC 1 (rules in StarRocks SQL) and POC 2 (Arrow Flight read, Stream Load write; Python and Rust) were dropped. At 50M
synthetic loci, this exchange through Parquet loaded fastest (one `FILES()` load in one transaction beats Stream
Loads), and keeps the rules in Python. A list of structs is ~15x slower to build in Polars than one struct column per
criterion; rules row by row in plain Python with Pydantic objects were ~38x slower than Polars.
