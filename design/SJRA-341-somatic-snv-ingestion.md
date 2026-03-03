# Technical Analysis: Somatic SNV Ingestion (Tumor-Normal)

---

## 1. Overview

This document describes the technical design for adding somatic SNV variant ingestion
into the Radiant portal pipeline, focusing exclusively on **tumor-normal bioinformatics
analysis** (paired VCFs with one tumor sample and one normal/germline sample).

A pseudocode prototype exists in branch `sandbox/jw-somatic-pseudocode`, but the ETL
has undergone significant architectural changes since that branch was created
(notably the [task-based processing](./SJRA-867-task-based-processing.md) and
[incremental loading](./SJRA-1187-pipeline-incremental-loading.md) migrations). This
document reconciles the pseudocode intent with the current architecture.

---

## 2. Context and Background

### 2.1 What Changed Since the Pseudocode

The pseudocode branch was written when the ETL used a `Case`/`Experiment` model. The
current ETL uses a task-based model (`BaseTask` → concrete task types). The key
differences are:

| Concern                | Pseudocode (old)                  | Current ETL                                                   |
|------------------------|-----------------------------------|---------------------------------------------------------------|
| Data unit              | `Case` with `experiments[]`       | `BaseTask` subclass with `experiments[]`                      |
| Identity key           | `case_id`                         | `task_id`                                                     |
| Deletion handling      | Not supported                     | `deleted` flag on tasks                                       |
| VCF DAG dispatch       | `@task.external_python` directly  | Operator abstraction (`k8s` / `ecs`)                          |
| StarRocks table prefix | `occurrence`, `variant`, etc.     | `germline__snv__occurrence`, `somatic__snv__occurrence`, etc. |
| Table registry         | Flat dicts in `radiant_tables.py` | Enum-based `RadiantConfigKeys` + structured dicts             |
| Import dispatch        | One static trigger                | Short-circuit + task-type filtering per task type             |

### 2.2 Tumor-Normal VCF Structure

A tumor-normal paired VCF contains exactly **two samples** per record:
- **Tumor** (typically column index 0 in CHOP convention, identified by aliquot)
- **Normal** (typically column index 1)

Each variant record contains FORMAT fields for each sample, plus shared INFO fields.

---

## 3. Current Germline SNV Architecture (Reference)

Understanding the germline flow is essential because somatic reuses parts of it.

```
import_part.py
  ├── fetch_sequencing_experiment_delta         (StarRocks → tasks with experiments)
  ├── [short-circuit: RADIANT_GERMLINE_ANNOTATION_TASK present?]
  ├── trigger: import_germline_snv_vcf          (sub-DAG, runs per task)
  │     └── process_case(task)
  │           ├── VCF → process_common()        → locus hash/chromosome/position
  │           ├── VCF → process_occurrence()    → germline_snv_occurrence (Iceberg)
  │           ├── VCF → process_variant()       → snv_variant (Iceberg, shared)
  │           └── VCF → process_consequence()   → snv_consequence (Iceberg, shared)
  ├── refresh_iceberg_tables                    (StarRocks REFRESH EXTERNAL TABLE)
  ├── insert_germline_snv_occurrence            (Iceberg → germline__snv__occurrence, swap partition)
  ├── insert_stg_snv_variant_freq               (staging frequency, germline rows)
  ├── aggregate_snv_variant_freq                (global frequency rollup)
  ├── tg_variants                               (staging → snv__variant → snv__variant_partitioned)
  └── tg_consequences                           (snv__consequence → filter → filter_part)
```

**Key principle**: Raw VCF data goes to **Iceberg** (parquet). StarRocks reads from Iceberg
via external tables and materializes it into its own partitioned tables using
`INSERT OVERWRITE` with `dynamic_overwrite = true`.

---

## 4. Proposed Somatic SNV Design

### 4.1 Scope for Tumor-Normal Analysis

- **Occurrences**: A new dedicated `somatic__snv__occurrence` table holds one row per VCF
  record with `tumor_*` / `normal_*` column pairs.
- **Variants and consequences**: Shared with germline. The existing
  `germline_snv_variant` / `germline_snv_consequence` tables (both Iceberg and StarRocks)
  are **renamed** to `snv_variant` / `snv_consequence` to reflect their shared ownership
  (see §4.6).
- **Variant frequency**: The shared `snv__variant_frequency` table gains new columns for
  tumor-normal frequencies alongside the existing germline frequency columns (see §4.7).
- No Exomiser integration for somatic.

### 4.2 New Task Type

A new task type must be introduced in `radiant/tasks/vcf/experiment.py`, following the
pattern of `RadiantGermlineAnnotationTask`:

```python
RADIANT_SOMATIC_ANNOTATION_TASK = "radiant_somatic_annotation"

class RadiantSomaticAnnotationTask(BaseTask):
    task_type: str = RADIANT_SOMATIC_ANNOTATION_TASK
    vcf_filepath: str
    index_vcf_filepath: str | None = None

    @staticmethod
    def gather_additional_args(rows: list[dict]) -> dict:
        for r in rows:
            if r["family_role"] == "tumor":   # see §5.1 for discussion
                return {
                    "vcf_filepath": r["vcf_filepath"],
                    "index_vcf_filepath": r.get("index_vcf_filepath"),
                }
        raise ValueError("No tumor sample found in rows for `radiant_somatic_annotation` task.")
```

> **Open question — tumor/normal identification (§5.1)**: The `family_role` field currently
> holds germline roles (`proband`, `mother`, `father`). For somatic, the clinical model
> needs to expose a mechanism to distinguish tumor from normal. See §5.1.

### 4.3 Tumor / Normal Sample Identification

The pseudocode assumes a fixed column order (tumor=index 0, normal=index 1), with a
comment flagging this as fragile. The recommended approach is:

1. The clinical model (upstream) assigns `family_role = "tumor"` / `"normal"` (or
   equivalent) to the somatic sequencing experiments.
2. `RadiantSomaticAnnotationTask.gather_additional_args` resolves which experiment is
   tumor vs. normal from the task rows.
3. `process_occurrence` resolves `tumor_idx` / `normal_idx` by matching
   `experiment.aliquot` against `vcf.samples` — not by hardcoded index.

If VCF sample IDs do not match aliquot IDs (known CHOP edge case), the pipeline should
**raise a clear error** rather than silently using positional order.

### 4.4 Data Flow

```
import_part.py
  ├── fetch_sequencing_experiment_delta
  ├── [short-circuit: RADIANT_GERMLINE_ANNOTATION_TASK present?]
  │   └── trigger: import_germline_snv_vcf      → germline_snv_occurrence (Iceberg)
  │                                             → snv_variant (Iceberg, shared)
  │                                             → snv_consequence (Iceberg, shared)
  ├── [short-circuit: RADIANT_SOMATIC_ANNOTATION_TASK present?]
  │   └── trigger: import_somatic_snv_vcf       → somatic_snv_occurrence (Iceberg)
  │                                             → snv_variant (Iceberg, shared)
  │                                             → snv_consequence (Iceberg, shared)
  ├── refresh_iceberg_tables                    (germline_snv_occurrence + somatic_snv_occurrence
  │                                              + snv_variant + snv_consequence)
  ├── insert_germline_snv_occurrence            (Iceberg → germline__snv__occurrence, swap partition)
  ├── insert_somatic_snv_occurrence             (Iceberg → somatic__snv__occurrence, swap partition)
  ├── insert_hashes + overwrite_snv_tmp_variant (shared locus hash → locus_id lookup)
  ├── insert_stg_snv_variant_freq               (staging frequency: germline + tumor-normal rows)
  ├── aggregate_snv_variant_freq                (global rollup → snv__variant_frequency)
  ├── tg_variants                               (staging → snv__variant → snv__variant_partitioned)
  └── tg_consequences                           (snv__consequence → filter → filter_part)
```

### 4.5 New Iceberg Table: `somatic_snv_occurrence`

The table stores the raw parsed VCF data for somatic. Schema is defined in
`radiant/tasks/vcf/snv/somatic/occurrence.py` as a PyIceberg `Schema` (extending
`COMMON_SCHEMA`).

Key fields (extending germline `COMMON_SCHEMA` which provides `locus_hash`, `chromosome`,
`start`, `end`, `reference`, `alternate`):

| Field                             | Type        | Notes                              |
|-----------------------------------|-------------|------------------------------------|
| `part`                            | `INT`       | Partition key                      |
| `task_id`                         | `INT`       | Replaces `case_id` from pseudocode |
| `quality`                         | `FLOAT`     | Record-level QUAL                  |
| `filter`                          | `STRING`    | Record-level FILTER                |
| `info_hotspotallele`              | `STRING`    | INFO/HotSpotAllele                 |
| `info_old_record`                 | `STRING`    | INFO/OLD_RECORD                    |
| `info_baseq_rank_sum`             | `FLOAT`     | INFO/BaseQRankSum                  |
| `info_excess_het`                 | `FLOAT`     | INFO/ExcessHet                     |
| `info_fs`                         | `FLOAT`     | INFO/FS                            |
| `info_ds`                         | `BOOL`      | INFO/DS                            |
| `info_fraction_informative_reads` | `FLOAT`     | INFO/FractionInformativeReads      |
| `info_inbreed_coeff`              | `FLOAT`     | INFO/InbreedCoeff                  |
| `info_mleac`                      | `INT`       | INFO/MLEAC                         |
| `info_mleaf`                      | `FLOAT`     | INFO/MLEAF                         |
| `info_mq`                         | `FLOAT`     | INFO/MQ                            |
| `info_mq0`                        | `FLOAT`     | INFO/MQ0                           |
| `info_m_qrank_sum`                | `FLOAT`     | INFO/MQRankSum                     |
| `info_qd`                         | `FLOAT`     | INFO/QD                            |
| `info_r2_5p_bias`                 | `FLOAT`     | INFO/R2_5P_bias                    |
| `info_read_pos_rank_sum`          | `FLOAT`     | INFO/ReadPosRankSum                |
| `info_sor`                        | `FLOAT`     | INFO/SOR                           |
| `info_vqslod`                     | `FLOAT`     | INFO/VQSLod                        |
| `info_culprit`                    | `STRING`    | INFO/Culprit                       |
| `info_dp`                         | `INT`       | INFO/DP                            |
| `info_haplotype_score`            | `FLOAT`     | INFO/HaplotypeScore                |
| `tumor_seq_id`                    | `INT`       | Tumor sample seq_id                |
| `tumor_calls`                     | `LIST<INT>` | Tumor genotype calls               |
| `tumor_dp`                        | `INT`       | Tumor FORMAT/DP                    |
| `tumor_gq`                        | `INT`       | Tumor FORMAT/GQ                    |
| `tumor_has_alt`                   | `BOOL`      | Has alternate allele               |
| `tumor_af`                        | `FLOAT`     | Tumor allele frequency             |
| `tumor_zygosity`                  | `STRING`    | HET/HOM/WT/UNK/HEM                 |
| `tumor_ad_ref`                    | `INT`       | Tumor ref allele depth             |
| `tumor_ad_alt`                    | `INT`       | Tumor alt allele depth             |
| `tumor_ad_total`                  | `INT`       | Tumor total depth                  |
| `tumor_ad_ratio`                  | `FLOAT`     | Tumor alt/total ratio              |
| `tumor_phased`                    | `BOOL`      | Tumor phased flag                  |
| `tumor_gt_status`                 | `STRING`    | Tumor genotype status (TBD, §5.4)  |
| `normal_seq_id`                   | `INT`       | Normal sample seq_id               |
| `normal_calls`                    | `LIST<INT>` | Normal genotype calls              |
| `normal_dp`                       | `INT`       | Normal FORMAT/DP                   |
| `normal_gq`                       | `INT`       | Normal FORMAT/GQ                   |
| `normal_has_alt`                  | `BOOL`      | Has alternate allele               |
| `normal_af`                       | `FLOAT`     | Normal allele frequency            |
| `normal_zygosity`                 | `STRING`    | HET/HOM/WT/UNK/HEM                 |
| `normal_ad_ref`                   | `INT`       | Normal ref allele depth            |
| `normal_ad_alt`                   | `INT`       | Normal alt allele depth            |
| `normal_ad_total`                 | `INT`       | Normal total depth                 |
| `normal_ad_ratio`                 | `FLOAT`     | Normal alt/total ratio             |
| `normal_phased`                   | `BOOL`      | Normal phased flag                 |
| `normal_gt_status`                | `STRING`    | Normal genotype status (TBD, §5.4) |

### 4.6 Shared Table Renames: `snv_variant` and `snv_consequence`

Since variants and consequences are locus-based and identical in structure for germline
and somatic, the existing `germline_snv_variant` and `germline_snv_consequence` tables
are renamed in **both Iceberg and StarRocks** to drop the `germline_` prefix. All
dependent tables follow suit.

#### Iceberg renames

| Old name                   | New name          |
|----------------------------|-------------------|
| `germline_snv_variant`     | `snv_variant`     |
| `germline_snv_consequence` | `snv_consequence` |

#### StarRocks renames

| Old name                                        | New name                              |
|-------------------------------------------------|---------------------------------------|
| `germline__snv__variant`                        | `snv__variant`                        |
| `germline__snv__consequence`                    | `snv__consequence`                    |
| `germline__snv__consequence_filter`             | `snv__consequence_filter`             |
| `germline__snv__consequence_filter_partitioned` | `snv__consequence_filter_partitioned` |
| `germline__snv__variant_frequency`              | `snv__variant_frequency`              |
| `germline__snv__staging_variant`                | `snv__staging_variant`                |
| `germline__snv__staging_variant_frequency_part` | `snv__staging_variant_frequency_part` |
| `germline__snv__tmp_variant`                    | `snv__tmp_variant`                    |
| `germline__snv__variant_partitioned`            | `snv__variant_partitioned`            |

> The occurrence tables are **not** renamed — they remain analysis-type-specific:
> `germline__snv__occurrence` and `somatic__snv__occurrence`.

#### `radiant_tables.py` key renames

Mapping keys in `STARROCKS_RADIANT_MAPPING` and `ICEBERG_GERMLINE_SNV_MAPPING` are
updated to reflect shared ownership:

| Old key                                                 | New key                                        |
|---------------------------------------------------------|------------------------------------------------|
| `iceberg_variant`                                       | `iceberg_snv_variant`                          |
| `iceberg_consequence`                                   | `iceberg_snv_consequence`                      |
| `starrocks_germline_snv_variant`                        | `starrocks_snv_variant`                        |
| `starrocks_germline_snv_consequence`                    | `starrocks_snv_consequence`                    |
| `starrocks_germline_snv_consequence_filter`             | `starrocks_snv_consequence_filter`             |
| `starrocks_germline_snv_consequence_filter_partitioned` | `starrocks_snv_consequence_filter_partitioned` |
| `starrocks_germline_snv_variant_frequency`              | `starrocks_snv_variant_frequency`              |
| `starrocks_germline_snv_staging_variant`                | `starrocks_snv_staging_variant`                |
| `starrocks_germline_snv_staging_variant_frequency`      | `starrocks_snv_staging_variant_frequency`      |
| `starrocks_germline_snv_tmp_variant`                    | `starrocks_snv_tmp_variant`                    |
| `starrocks_germline_snv_variant_partitioned`            | `starrocks_snv_variant_partitioned`            |

The `ICEBERG_GERMLINE_SNV_MAPPING` dict is split into three:

```python
# Occurrence tables — remain analysis-type-specific
ICEBERG_GERMLINE_SNV_MAPPING = {
    "iceberg_germline_snv_occurrence": "germline_snv_occurrence",
    "iceberg_germline_cnv_occurrence":  "germline_cnv_occurrence",
}

# Shared variant / consequence tables
ICEBERG_SNV_MAPPING = {
    "iceberg_snv_variant":    "snv_variant",
    "iceberg_snv_consequence": "snv_consequence",
}

# Somatic occurrence table
ICEBERG_SOMATIC_SNV_MAPPING = {
    "iceberg_somatic_snv_occurrence": "somatic_snv_occurrence",
}
```

### 4.7 Variant Frequency: New Somatic Columns

The `snv__variant_frequency` and `snv__staging_variant_frequency_part` tables gain new
columns for tumor-normal frequencies. The existing germline columns are unchanged.

#### Germline columns (renamed)

`pc` = patient carrier count, `pn` = total patients, `pf` = frequency (`pc / pn`).

| Old column name       | New column name                | Scope                                          |
|-----------------------|--------------------------------|------------------------------------------------|
| `pc_wgs`              | `pc_germline_wgs`              | All WGS germline patients carrying the variant |
| `pn_wgs`              | `pn_germline_wgs`              | Total WGS germline patients                    |
| `pf_wgs`              | `pf_germline_wgs`              | WGS germline frequency                         |
| `pc_wgs_affected`     | `pc_germline_wgs_affected`     | WGS affected carriers                          |
| `pn_wgs_affected`     | `pn_germline_wgs_affected`     | Total WGS affected patients                    |
| `pf_wgs_affected`     | `pf_germline_wgs_affected`     | WGS affected frequency                         |
| `pc_wgs_not_affected` | `pc_germline_wgs_not_affected` | WGS non-affected carriers                      |
| `pn_wgs_not_affected` | `pn_germline_wgs_not_affected` | Total WGS non-affected patients                |
| `pf_wgs_not_affected` | `pf_germline_wgs_not_affected` | WGS non-affected frequency                     |
| `pc_wxs`              | `pc_germline_wxs`              | All WXS germline patients carrying the variant |
| `pn_wxs`              | `pn_germline_wxs`              | Total WXS germline patients                    |
| `pf_wxs`              | `pf_germline_wxs`              | WXS germline frequency                         |
| `pc_wxs_affected`     | `pc_germline_wxs_affected`     | WXS affected carriers                          |
| `pn_wxs_affected`     | `pn_germline_wxs_affected`     | Total WXS affected patients                    |
| `pf_wxs_affected`     | `pf_germline_wxs_affected`     | WXS affected frequency                         |
| `pc_wxs_not_affected` | `pc_germline_wxs_not_affected` | WXS non-affected carriers                      |
| `pn_wxs_not_affected` | `pn_germline_wxs_not_affected` | Total WXS non-affected patients                |
| `pf_wxs_not_affected` | `pf_germline_wxs_not_affected` | WXS non-affected frequency                     |

#### New tumor-normal columns

`pc_tn` = count of **tumor-normal pairs** where `tumor_has_alt = TRUE`.
`pn_tn` = total tumor-normal pairs in the cohort. `pf_tn` = `pc_tn / pn_tn`.

| Column      | Type     | Description                                 |
|-------------|----------|---------------------------------------------|
| `pc_tn_wgs` | `BIGINT` | Tumor-normal WGS pairs carrying the variant |
| `pn_tn_wgs` | `BIGINT` | Total tumor-normal WGS pairs in cohort      |
| `pf_tn_wgs` | `DOUBLE` | Tumor-normal WGS frequency                  |
| `pc_tn_wxs` | `BIGINT` | Tumor-normal WXS pairs carrying the variant |
| `pn_tn_wxs` | `BIGINT` | Total tumor-normal WXS pairs in cohort      |
| `pf_tn_wxs` | `DOUBLE` | Tumor-normal WXS frequency                  |

#### Future tumor-only columns (deferred)

| Column      | Type     | Description                                 |
|-------------|----------|---------------------------------------------|
| `pc_to_wgs` | `BIGINT` | Tumor-only WGS samples carrying the variant |
| `pn_to_wgs` | `BIGINT` | Total tumor-only WGS samples                |
| `pf_to_wgs` | `DOUBLE` | Tumor-only WGS frequency                    |
| `pc_to_wxs` | `BIGINT` | Tumor-only WXS samples carrying the variant |
| `pn_to_wxs` | `BIGINT` | Total tumor-only WXS samples                |
| `pf_to_wxs` | `DOUBLE` | Tumor-only WXS frequency                    |

#### Staging frequency insert changes

`snv_staging_variant_freq_insert.sql` must be extended to also compute tumor-normal
counts by joining `somatic__snv__occurrence` (filtered on `tumor_has_alt = TRUE`) against
`staging_sequencing_experiment` for the WGS/WXS breakdown. The staging table
`snv__staging_variant_frequency_part` receives the new `pc_tn_wgs`, `pn_tn_wgs`,
`pc_tn_wxs`, `pn_tn_wxs` columns so the global rollup in
`snv_variant_frequency_insert.sql` can sum them across partitions.

### 4.8 New StarRocks Table: `somatic__snv__occurrence`

```sql
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_somatic_snv_occurrence }} (
    part             INT NOT NULL,
    task_id          INT NOT NULL,
    locus_id         BIGINT NOT NULL,
    tumor_seq_id     INT NOT NULL,
    normal_seq_id    INT,
    quality          FLOAT,
    filter           VARCHAR(255),
    -- INFO fields
    info_hotspotallele              VARCHAR(255),
    info_old_record                 VARCHAR(2000),
    info_baseq_rank_sum             FLOAT,
    info_excess_het                 FLOAT,
    info_fs                         FLOAT,
    info_ds                         BOOLEAN,
    info_fraction_informative_reads FLOAT,
    info_inbreed_coeff              FLOAT,
    info_mleac                      INT,
    info_mleaf                      FLOAT,
    info_mq                         FLOAT,
    info_mq0                        FLOAT,
    info_m_qrank_sum                FLOAT,
    info_qd                         FLOAT,
    info_r2_5p_bias                 FLOAT,
    info_read_pos_rank_sum          FLOAT,
    info_sor                        FLOAT,
    info_vqslod                     FLOAT,
    info_culprit                    VARCHAR(255),
    info_dp                         INT,
    info_haplotype_score            FLOAT,
    -- Tumor FORMAT
    tumor_calls      ARRAY<INT>,
    tumor_dp         INT,
    tumor_gq         INT,
    tumor_has_alt    BOOLEAN,
    tumor_af         FLOAT,
    tumor_zygosity   CHAR(3),
    tumor_ad_ref     INT,
    tumor_ad_alt     INT,
    tumor_ad_total   INT,
    tumor_ad_ratio   FLOAT,
    tumor_phased     BOOLEAN NOT NULL,
    tumor_gt_status  VARCHAR(50),
    -- Normal FORMAT
    normal_calls     ARRAY<INT>,
    normal_dp        INT,
    normal_gq        INT,
    normal_has_alt   BOOLEAN,
    normal_af        FLOAT,
    normal_zygosity  CHAR(3),
    normal_ad_ref    INT,
    normal_ad_alt    INT,
    normal_ad_total  INT,
    normal_ad_ratio  FLOAT,
    normal_phased    BOOLEAN NOT NULL,
    normal_gt_status VARCHAR(50),
    INDEX locus_id_index (`locus_id`) USING BITMAP COMMENT ''
)
ENGINE=OLAP
DUPLICATE KEY(`part`, `task_id`, `tumor_seq_id`, `locus_id`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ mapping.colocate_query_group }}"
);
```

### 4.9 SQL: Iceberg → StarRocks Somatic Occurrence Insert

`radiant/dags/sql/radiant/somatic_snv_occurrence_insert_partition_delta.sql`

```sql
SELECT
    o.part,
    o.task_id,
    v.locus_id,
    o.tumor_seq_id,
    o.normal_seq_id,
    o.quality,
    o.filter,
    o.info_hotspotallele,
    o.info_old_record,
    o.info_baseq_rank_sum,
    o.info_excess_het,
    o.info_fs,
    o.info_ds,
    o.info_fraction_informative_reads,
    o.info_inbreed_coeff,
    o.info_mleac,
    o.info_mleaf,
    o.info_mq,
    o.info_mq0,
    o.info_m_qrank_sum,
    o.info_qd,
    o.info_r2_5p_bias,
    o.info_read_pos_rank_sum,
    o.info_sor,
    o.info_vqslod,
    o.info_culprit,
    o.info_dp,
    o.info_haplotype_score,
    o.tumor_calls,
    o.tumor_dp,
    o.tumor_gq,
    o.tumor_has_alt,
    o.tumor_af,
    o.tumor_zygosity,
    o.tumor_ad_ref,
    o.tumor_ad_alt,
    o.tumor_ad_total,
    o.tumor_ad_ratio,
    o.tumor_phased,
    o.tumor_gt_status,
    o.normal_calls,
    o.normal_dp,
    o.normal_gq,
    o.normal_has_alt,
    o.normal_af,
    o.normal_zygosity,
    o.normal_ad_ref,
    o.normal_ad_alt,
    o.normal_ad_total,
    o.normal_ad_ratio,
    o.normal_phased,
    o.normal_gt_status
FROM {{ mapping.iceberg_somatic_snv_occurrence }} o
JOIN [BROADCAST] {{ mapping.starrocks_snv_tmp_variant }} v
    ON o.locus_hash = v.locus_hash
WHERE o.part = {{ partition }}
  AND o.task_id IN %(task_ids)s
  AND COALESCE(o.tumor_has_alt, FALSE);
```

### 4.10 New DAG: `import_somatic_snv_vcf.py`

Mirrors `import_germline_snv_vcf.py` but:
- Filters for `RADIANT_SOMATIC_ANNOTATION_TASK` task type instead of
  `RADIANT_GERMLINE_ANNOTATION_TASK`.
- Calls `radiant.tasks.vcf.snv.somatic.process.process_case` which writes to
  `somatic_snv_occurrence`, `snv_variant`, and `snv_consequence` in Iceberg.
- Uses the same k8s / ECS operator abstraction pattern.

### 4.11 Changes to `import_part.py`

1. **Trigger somatic VCF import** in addition to germline, guarded by a short-circuit:

```python
import_somatic_vcf = TriggerDagRunOperator(
    task_id="import_somatic_snv_vcf",
    task_display_name="[DAG] Import Somatic SNV VCF into Iceberg",
    trigger_dag_id=f"{NAMESPACE}-import-somatic-snv-vcf",
    conf=_prepare_config,
    reset_dag_run=True,
    wait_for_completion=True,
    poke_interval=2,
)
```

2. **Short-circuit guard**: Skip if no somatic tasks in the partition (analogous to
   `sanity_check_cnvs`).

3. **`get_tables_to_refresh`**: Update to use renamed mapping keys (`iceberg_snv_variant`,
   `iceberg_snv_consequence`) and add `iceberg_somatic_snv_occurrence`.

4. **`insert_somatic_snv_occurrence`**: New `RadiantStarRocksPartitionSwapOperator` that
   runs in parallel with `insert_germline_snv_occurrence`, both feeding into the shared
   frequency and variant steps downstream.

### 4.12 Changes to `radiant_tables.py`

```python
# ICEBERG_SNV_MAPPING (new — shared variant/consequence tables)
ICEBERG_SNV_MAPPING = {
    "iceberg_snv_variant":     "snv_variant",
    "iceberg_snv_consequence":  "snv_consequence",
}

# ICEBERG_SOMATIC_SNV_MAPPING (new)
ICEBERG_SOMATIC_SNV_MAPPING = {
    "iceberg_somatic_snv_occurrence": "somatic_snv_occurrence",
}

# ICEBERG_GERMLINE_SNV_MAPPING (reduced — occurrence tables only)
ICEBERG_GERMLINE_SNV_MAPPING = {
    "iceberg_germline_snv_occurrence": "germline_snv_occurrence",
    "iceberg_germline_cnv_occurrence":  "germline_cnv_occurrence",
}

# STARROCKS_RADIANT_MAPPING — renamed shared keys + new somatic entry
# (see §4.6 for the full rename table)
"starrocks_snv_variant":                        "snv__variant",
"starrocks_snv_consequence":                    "snv__consequence",
"starrocks_snv_consequence_filter":             "snv__consequence_filter",
"starrocks_snv_consequence_filter_partitioned": "snv__consequence_filter_partitioned",
"starrocks_snv_variant_frequency":              "snv__variant_frequency",
"starrocks_snv_staging_variant":                "snv__staging_variant",
"starrocks_snv_staging_variant_frequency":      "snv__staging_variant_frequency_part",
"starrocks_snv_tmp_variant":                    "snv__tmp_variant",
"starrocks_snv_variant_partitioned":            "snv__variant_partitioned",
"starrocks_somatic_snv_occurrence":             "somatic__snv__occurrence",
```

---

## 5. Open Questions

### 5.1 Tumor/Normal Role Convention

**How does the clinical model communicate which sample is the tumor vs. the normal?**

Options:
- **A)** Use a new `family_role` value (`"tumor"` / `"normal"`) — minimal clinical model
  change, consistent with existing role-based logic in `_build_experiments`.
- **B)** Use a new `histology_type` field.
- **C)** Use VCF sample ordering with a validated fallback error when aliquots don't match.

Option **B** is recommended.

### 5.2 Occurrence Row Granularity

The pseudocode generates **one occurrence row per VCF record**, keyed by `tumor_seq_id`.
Normal sample data is embedded as `normal_*` columns in the same row.

Alternative: two rows per record (one per sample, using a single `seq_id` column like
germline). This would allow reusing the germline occurrence schema and filtering logic
but loses the paired tumor-normal context within a single row.

**Recommendation**: Keep one row per record with `tumor_*` / `normal_*` columns. This
preserves the somatic context and enables queries like "variants present in tumor but
absent in normal" without self-joins.

### 5.3 `adjust_calls_and_zygosity` for Somatic

The pseudocode copies `adjust_calls_and_zygosity` from germline with a note that it
needs somatic-specific review. In somatic VCFs (especially GATK Mutect2), the zygosity
and depth thresholds may differ significantly. The threshold of `ad_alt < 3` used in
germline may be inappropriate for somatic variants with low VAF.

This function should be reviewed against the actual somatic caller specifications before
implementation.

### 5.4 `tumor_gt_status` / `normal_gt_status`

These fields are stubbed as `None` in the pseudocode. Define what values these should
take (e.g., PASS/FILTER/NOCALL) and implement the logic.

### 5.5 Tumor-Normal Frequency Filter Criteria

The germline staging frequency query filters occurrences with `gq >= 20 AND filter='PASS'
AND ad_alt > 3`. The somatic equivalent criteria need to be defined — for example,
filtering on `tumor_has_alt = TRUE` and possibly `tumor_gq` / `tumor_ad_alt` thresholds
appropriate to the somatic caller.

---

## 6. Files to Create / Modify

| Action              | File                                                                                                                                                                                          |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Create**          | `radiant/tasks/vcf/snv/somatic/__init__.py`                                                                                                                                                   |
| **Create**          | `radiant/tasks/vcf/snv/somatic/occurrence.py`                                                                                                                                                 |
| **Create**          | `radiant/tasks/vcf/snv/somatic/process.py`                                                                                                                                                    |
| **Create**          | `radiant/dags/import_somatic_snv_vcf.py`                                                                                                                                                      |
| **Create**          | `radiant/dags/sql/radiant/somatic_snv_occurrence_insert_partition_delta.sql`                                                                                                                  |
| **Create**          | `radiant/dags/sql/radiant/init/somatic_snv_occurrence_create_table.sql`                                                                                                                       |
| **Modify**          | `radiant/tasks/vcf/experiment.py` — add `RADIANT_SOMATIC_ANNOTATION_TASK` + `RadiantSomaticAnnotationTask`                                                                                    |
| **Modify**          | `radiant/tasks/data/radiant_tables.py` — split Iceberg mapping dicts, rename StarRocks keys, add somatic entries                                                                              |
| **Modify**          | `radiant/dags/import_part.py` — trigger somatic DAG, short-circuit guard, somatic occurrence insert, updated table refresh keys                                                               |
| **Modify**          | `radiant/dags/import_germline_snv_vcf.py` — update Iceberg table target names (`snv_variant`, `snv_consequence`)                                                                              |
| **Modify**          | `radiant/dags/init_iceberg_tables.py` — rename variant/consequence tables, register `somatic_snv_occurrence`                                                                                  |
| **Modify**          | `radiant/dags/init_starrocks_tables.py` — rename all shared `germline__snv__*` tables, add `somatic__snv__occurrence`                                                                         |
| **Rename + Modify** | `germline_snv_staging_variant_freq_insert.sql` → `snv_staging_variant_freq_insert.sql` — rename germline columns (`pc_wgs` → `pc_germline_wgs`, etc.), add tumor-normal frequency computation |
| **Rename + Modify** | `germline_snv_variant_frequency_insert.sql` → `snv_variant_frequency_insert.sql` — rename germline columns, add `pc_tn_*` / `pn_tn_*` / `pf_tn_*` columns                                     |
| **Rename + Modify** | `init/germline_snv_variant_frequency_create_table.sql` → `init/snv_variant_frequency_create_table.sql` — rename germline columns, add new frequency columns                                   |
| **Rename + Modify** | `init/germline_snv_staging_variant_frequency_create_table.sql` → `init/snv_staging_variant_frequency_create_table.sql` — rename germline columns, add new frequency columns                   |
| **Modify**          | `init/germline_snv_variant_create_table.sql` → `init/snv_variant_create_table.sql` — rename inline frequency columns (`pf_wgs` → `pf_germline_wgs`, `pc_wgs` → `pc_germline_wgs`, etc.)       |
| **Rename**          | All other `germline_snv_*` SQL files referencing shared tables — update mapping key references throughout                                                                                     |

---

## 7. Out of Scope

- Tumor-only analysis (no matched normal): not covered in this design.
- Somatic CNV ingestion: separate effort.
- Tumor-only frequency columns (`pc_to_*`): deferred, columns documented in §4.7 for future implementation.
- Exomiser integration: not applicable for somatic.


## Appendix: SQL migration code

```
-- Consequence tables
ALTER TABLE radiant.germline__snv__consequence RENAME snv__consequence;
ALTER TABLE radiant.germline__snv__consequence_filter RENAME snv__consequence_filter;
ALTER TABLE radiant.germline__snv__consequence_filter_partitioned RENAME snv__consequence_filter_partitioned;

-- Variant tables
ALTER TABLE radiant.germline__snv__variant RENAME snv__variant;
ALTER TABLE radiant.germline__snv__staging_variant RENAME snv__staging_variant;
ALTER TABLE radiant.germline__snv__tmp_variant RENAME snv__tmp_variant;
ALTER TABLE radiant.germline__snv__variant_partitioned RENAME snv__variant_partitioned; 

-- Frequencies (table and column renames)
ALTER TABLE radiant.germline__snv__variant_frequency RENAME snv__variant_frequency;
ALTER TABLE radiant.germline__snv__staging_variant_frequency_part RENAME snv__staging_variant_frequency_part;

ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pc_wgs TO pc_germline_wgs;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pn_wgs TO pn_germline_wgs;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pf_wgs TO pf_germline_wgs;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pc_wgs_affected TO pc_germline_wgs_affected;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pn_wgs_affected TO pn_germline_wgs_affected;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pf_wgs_affected TO pf_germline_wgs_affected;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pc_wgs_not_affected TO pc_germline_wgs_not_affected;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pn_wgs_not_affected TO pn_germline_wgs_not_affected;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pf_wgs_not_affected TO pf_germline_wgs_not_affected;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pc_wxs TO pc_germline_wxs;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pn_wxs TO pn_germline_wxs;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pf_wxs TO pf_germline_wxs;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pc_wxs_affected TO pc_germline_wxs_affected;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pn_wxs_affected TO pn_germline_wxs_affected;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pf_wxs_affected TO pf_germline_wxs_affected;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pc_wxs_not_affected TO pc_germline_wxs_not_affected;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pn_wxs_not_affected TO pn_germline_wxs_not_affected;
ALTER TABLE radiant.snv__variant_frequency RENAME COLUMN pf_wxs_not_affected TO pf_germline_wxs_not_affected;
```