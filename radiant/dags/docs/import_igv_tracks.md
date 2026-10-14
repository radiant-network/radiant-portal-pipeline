# Importing the public IGV tracks

Builds the public reference tracks of the portal's IGV **Tracks** menu (RAD-30) and writes them, each with
its tabix index, under one S3 prefix. The portal API presigns them in the **public_tracks** field of its IGV
endpoint. Files only: no StarRocks or Iceberg table is read or written.

A port of Qlin's **etl_import_igv** DAG: same 11 upstream files, same output file names. The files are the
same for every case, so the DAG runs once per environment, by hand. Rerun it to take a new upstream release.

---

## Input

| Param | Type | Description |
|:--|:--|:--|
| **igv_tracks_prefix** | string | S3 prefix to write under, e.g. s3://(tracks bucket)/igv/. Defaults to the **RADIANT_IGV_TRACKS_S3_PREFIX** environment variable of the Airflow deployment. The task role must be able to **write** there. A rerun replaces every file. |

The DAG fails on its first task when the prefix is empty, is not an s3:// URI, or holds a character other
than letters, digits and . \_ - / (on ECS the prefix is passed through a shell command).

## Steps

| # | Task | Where | What it does |
|:--|:--|:--|:--|
| 1 | **list_igv_tracks** | Airflow | Checks the prefix and lists the 11 tracks. |
| 2 | **build_igv_track** (mapped, one per track) | radiant-operator pod (K8s) or task (ECS) | Downloads the upstream file, converts it to GFF3 when needed, sorts it, compresses it with bgzip, indexes it with tabix, then writes the file and its .tbi under the prefix. At most 4 at a time; each one retries twice, 5 minutes apart. |

A failed track does not stop the others. To retry one track, clear its mapped instance.

The data file and its .tbi are two uploads, data first. If the index upload fails, the new data file sits
next to the previous index until the retry rebuilds the track, and IGV reads wrong offsets for that track in
the meantime. A rerun of the failed track fixes it.

The four VCFs are already BGZF upstream, so they are copied as is and only indexed. A row of a UCSC table that
cannot be read fails its track with the row number; a byte that is not UTF-8 passes through unchanged. A GFF3
track with no feature at all (e.g. after an upstream format change) fails before its upload, so the previous
file stays in place.

## Tracks

| Track id | File written | Upstream | Conversion |
|:--|:--|:--|:--|
| clinvar | clinvar.vcf.gz | NCBI ClinVar VCF, GRCh38 | none |
| clinvar_sv | nstd102.GRCh38.variant_call.vcf.gz | NCBI dbVar, study nstd102 | none |
| gnomad_sv | gnomad.v4.1.sv.sites.vcf.gz | gnomAD 4.1 genome SV | none |
| gnomad_cnv | gnomad.v4.1.cnv.all.vcf.gz | gnomAD 4.1 exome CNV | none |
| dgv | DGV_GS_hg38.cleaned.sorted.gff3.gz | DGV gold standard, hg38 (GFF3) | stray % encoded as %25; gff-version header added |
| clingen_gene | ClinGen_gene_curation_list_GRCh38.sorted.gff3.gz | ClinGen gene dosage curation list, GRCh38 | TSV to GFF3 |
| clingen_region | ClinGen_region_curation_list_GRCh38.sorted.gff3.gz | ClinGen region dosage curation list, GRCh38 | TSV to GFF3; regions at location "tbd" are skipped |
| segmental_duplications | hg38.genomicSuperDups.gff3.gz | UCSC genomicSuperDups | table to GFF3 |
| repeat_masker | hg38.rmsk.gff3.gz | UCSC rmsk | table to GFF3 |
| simple_repeats | hg38.simpleRepeat.gff3.gz | UCSC simpleRepeat | table to GFF3 |
| self_chain | hg38.chainSelf.gff3.gz | UCSC chainSelf | table to GFF3 |

The track ids are the keys of **public_tracks** in the portal API; the portal backend lists the same file
names. Change both sides together.

## Differences with the Qlin DAG

The content is the same. These changes make the files valid for tabix and GFF3 (the docstring of
radiant/tasks/open_data/igv_tracks.py points here):

- Every GFF3 file is sorted on chromosome and start. The UCSC tables are ordered by bin, not by start.
- The VCFs are indexed as VCF. Qlin indexes them with the GFF preset.
- The VCFs are already BGZF upstream (all four), so they are copied as is instead of recompressed.
- GFF3 attribute values encode the reserved characters (semicolon, equals, ampersand, comma, percent, tab).
  igv.js decodes exactly these, so the values the portal colors on read as published.
- The segmental duplications and simple repeats features get the type segmental\_duplication and
  tandem\_repeat. Qlin writes an empty type column.
- A UCSC strand other than + or - is written as a dot (GFF3 allows + - . ? only). The hg38 rmsk and
  genomicSuperDups tables hold only + and -, so no row changes today.
- DGV gets the gff-version header line, which upstream lacks. The ClinGen files keep one comment line naming
  their source; Qlin's longer comment block is not carried.
- ClinGen is read over HTTPS. ftp.clinicalgenome.org serves both; Qlin's curl falls back to FTP.
- Like Qlin, at most 4 tracks run at a time. Each track also retries twice here.

## Resources

The largest track is self_chain: a 0.8 GB download that becomes about 5 GB of GFF3 to sort (33 million
rows). On K8s each pod requests 16Gi of ephemeral storage (override:
**RADIANT_TASK_OPERATOR_IGV_TRACKS_EPHEMERAL_STORAGE**). On ECS each task sets 30 GiB of Fargate ephemeral
storage in its run overrides, whatever the radiant-operator task definition says (fixed in the DAG, 21 to 200;
needs Fargate platform 1.4.0 or later).
The measured peak is 6.6 GB.

When a disk fills up, the task fails with the error of sort or bgzip (for example "No space left on
device"), not with a broken pipe.

## Triggered from radiant-import-open-data

**radiant-import-open-data** accepts **import_igv_tracks** (boolean, default false). When true, the import
triggers this DAG after its other sources and waits for it; the prefix comes from this DAG's default. It is
off by default because **radiant-reannotate-open-data** triggers the import and must not rebuild the files
each time.

A failed track fails this run, and so the import run too. The open-data tables of that import are already
loaded and recorded, so do not rerun the import: clear the failed **build_igv_track** instance here, or
trigger this DAG alone.

> Example conf for radiant-import-open-data: {"import_igv_tracks": true}
