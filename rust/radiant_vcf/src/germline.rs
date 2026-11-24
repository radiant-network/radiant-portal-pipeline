//! Germline SNV extraction: port of `radiant/tasks/vcf/snv/germline/process.py` and the modules it calls
//! (`occurrence.py`, `comphet.py`, `inheritance.py`, `snv/common.py`, `snv/variant.py`, `snv/consequence.py`).
//!
//! Records are decoded once and appended to Arrow builders, one per output table. The Python side casts each
//! batch to the Iceberg schema of its table and hands it to `TableAccumulator`, so the Parquet writing and
//! the Iceberg commit stay unchanged.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::CString;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Int32Builder, ListBuilder, StringBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use rust_htslib::bcf::header::HeaderRecord;
use rust_htslib::bcf::{self, Read};
use rust_htslib::htslib;
use sha2::{Digest, Sha256};

use crate::consequence::{self, Consequence, CsqIndexes, CsqStats};
use crate::genotype::{self, DepthSource};
use crate::hts::{self, I32Buf, InfoValue, cstring};
use crate::inheritance::{self, SlivarTags};

const SUPPORTED_CHROMOSOMES: [&str; 25] = [
    "chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11", "chr12", "chr13",
    "chr14", "chr15", "chr16", "chr17", "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY", "chrM",
];

const PROGENY_ROLES: [&str; 4] = ["proband", "brother", "sister", "sibling"];

/// `Experiment`, reduced to the fields the extraction reads.
#[derive(Debug, Clone)]
pub struct Experiment {
    pub aliquot: String,
    pub seq_id: i32,
    pub tenant_code: String,
    pub family_role: String,
    pub affected_status: String,
    pub sex: String,
}

/// `Pedigree`: the experiments in VCF sample order, and the family roles.
struct Pedigree {
    experiments: Vec<Experiment>,
    father_seq_id: Option<i32>,
    mother_seq_id: Option<i32>,
    is_father_affected: bool,
    is_mother_affected: bool,
    /// Indexes into `experiments`.
    progenies: Vec<usize>,
    is_family: bool,
}

impl Pedigree {
    fn new(task_experiments: &[Experiment], vcf_samples: &[String]) -> Self {
        let experiments: Vec<Experiment> = vcf_samples
            .iter()
            .filter_map(|s| task_experiments.iter().find(|e| &e.aliquot == s).cloned())
            .collect();
        let father = experiments.iter().find(|e| e.family_role == "father");
        let mother = experiments.iter().find(|e| e.family_role == "mother");
        let progenies: Vec<usize> = experiments
            .iter()
            .enumerate()
            .filter(|(_, e)| PROGENY_ROLES.contains(&e.family_role.as_str()))
            .map(|(i, _)| i)
            .collect();
        // Python truthiness: a seq_id of 0 counts as no parent.
        let has_parent = father.is_some_and(|f| f.seq_id != 0) || mother.is_some_and(|m| m.seq_id != 0);
        Self {
            father_seq_id: father.map(|f| f.seq_id),
            mother_seq_id: mother.map(|m| m.seq_id),
            is_father_affected: father.is_some_and(|f| f.affected_status == "affected"),
            is_mother_affected: mother.is_some_and(|m| m.affected_status == "affected"),
            is_family: has_parent && !progenies.is_empty(),
            progenies,
            experiments,
        }
    }
}

/// The INFO fields of the occurrence table: (column, name the ETL reads it under, column type).
#[derive(Clone, Copy)]
enum Col {
    F32,
    I32,
    Bool,
    Str,
}

const OCC_INFO: [(&str, &str, Col); 19] = [
    ("info_old_record", "OLD_RECORD", Col::Str),
    ("info_baseq_rank_sum", "BaseQRankSum", Col::F32),
    ("info_excess_het", "ExcessHet", Col::F32),
    ("info_fs", "FS", Col::F32),
    ("info_ds", "DS", Col::Bool),
    ("info_fraction_informative_reads", "FractionInformativeReads", Col::F32),
    ("info_inbreed_coeff", "InbreedingCoeff", Col::F32),
    ("info_mleac", "MLEAC", Col::I32),
    ("info_mleaf", "MLEAF", Col::F32),
    ("info_mq", "MQ", Col::F32),
    ("info_m_qrank_sum", "MQRankSum", Col::F32),
    ("info_qd", "QD", Col::F32),
    ("info_r2_5p_bias", "R2_5P_bias", Col::F32),
    ("info_read_pos_rank_sum", "ReadPosRankSum", Col::F32),
    ("info_sor", "SOR", Col::F32),
    ("info_vqslod", "VQSLOD", Col::F32),
    ("info_culprit", "culprit", Col::Str),
    ("info_dp", "DP", Col::I32),
    ("info_haplotype_score", "HaplotypeScore", Col::F32),
];

/// `INFO_ALIASES` of `vcf_utils.py`.
fn info_aliases(name: &str) -> &'static [&'static str] {
    if name == "InbreedingCoeff" {
        &["InbreedCoeff"]
    } else {
        &[]
    }
}

/// One INFO key as the header declares it, with what reading it needs.
#[derive(Clone)]
struct InfoKey {
    key: CString,
    is_flag: bool,
}

/// The value of one INFO field in a typed column, or the error pyarrow raises for it.
#[derive(Clone)]
enum Scalar {
    F32(Option<f32>),
    I32(Option<i32>),
    Bool(Option<bool>),
    Str(Option<String>),
}

fn to_column(v: Option<InfoValue>, col: Col, name: &str) -> Result<Scalar, String> {
    let bad = |v: &InfoValue| Err(format!("INFO/{name} value {v:?} does not fit its column"));
    Ok(match col {
        Col::F32 => Scalar::F32(match v {
            None => None,
            Some(InfoValue::Float(f)) => Some(f),
            Some(InfoValue::Int(i)) => Some(i as f32),
            Some(other) => return bad(&other),
        }),
        Col::I32 => Scalar::I32(match v {
            None => None,
            Some(InfoValue::Int(i)) => Some(i32::try_from(i).map_err(|_| format!("INFO/{name} {i} overflows int32"))?),
            Some(other) => return bad(&other),
        }),
        Col::Bool => Scalar::Bool(match v {
            None => None,
            Some(InfoValue::Bool(b)) => Some(b),
            Some(other) => return bad(&other),
        }),
        Col::Str => Scalar::Str(match v {
            None => None,
            Some(InfoValue::Str(s)) => Some(s),
            Some(other) => return bad(&other),
        }),
    })
}

/// `snv/common.py::locus_and_hash`.
pub fn locus_and_hash(chrom: &str, pos: i64, reference: &str, alt: &str) -> (String, String) {
    let locus = format!("{chrom}-{pos}-{reference}-{alt}");
    let hash = hex::encode(Sha256::digest(locus.as_bytes()));
    (locus, hash)
}

/// `Common`.
struct Common {
    locus: String,
    locus_hash: String,
    chromosome: String,
    start: i32,
    end: i32,
    reference: String,
    alternate: String,
}

/// One sample's occurrence, before the family columns.
struct Occ {
    dp: Option<i32>,
    gq: Option<i32>,
    calls: Vec<i32>,
    has_alt: bool,
    zygosity: &'static str,
    ad_ref: Option<i32>,
    ad_alt: Option<i32>,
    ad_total: Option<i32>,
    ad_ratio: Option<f32>,
    phased: bool,
}

/// The family columns of a progeny's occurrence. A non-progeny row leaves all of them null.
#[derive(Default)]
struct Family {
    parental_origin: Option<&'static str>,
    parental_origin_source: Option<&'static str>,
    transmission_mode: Option<&'static str>,
    other_transmission_modes: Option<Vec<&'static str>>,
    transmission_mode_candidate: Option<bool>,
    transmission_mode_source: Option<&'static str>,
}

// ---------------------------------------------------------------------------------------------------------------
// Arrow builders
// ---------------------------------------------------------------------------------------------------------------

fn list_of(item: DataType) -> Field {
    // Iceberg's list element is named `element` and is required.
    Field::new("element", item, false)
}

struct CommonCols {
    task_id: Int32Builder,
    locus: StringBuilder,
    locus_hash: StringBuilder,
    chromosome: StringBuilder,
    start: Int32Builder,
    end: Int32Builder,
    reference: StringBuilder,
    alternate: StringBuilder,
}

impl CommonCols {
    fn new() -> Self {
        Self {
            task_id: Int32Builder::new(),
            locus: StringBuilder::new(),
            locus_hash: StringBuilder::new(),
            chromosome: StringBuilder::new(),
            start: Int32Builder::new(),
            end: Int32Builder::new(),
            reference: StringBuilder::new(),
            alternate: StringBuilder::new(),
        }
    }

    fn append(&mut self, task_id: i32, c: &Common) {
        self.task_id.append_value(task_id);
        self.locus.append_value(&c.locus);
        self.locus_hash.append_value(&c.locus_hash);
        self.chromosome.append_value(&c.chromosome);
        self.start.append_value(c.start);
        self.end.append_value(c.end);
        self.reference.append_value(&c.reference);
        self.alternate.append_value(&c.alternate);
    }

    fn finish(&mut self, cols: &mut Vec<(&'static str, ArrayRef)>) {
        cols.push(("task_id", Arc::new(self.task_id.finish())));
        cols.push(("locus", Arc::new(self.locus.finish())));
        cols.push(("locus_hash", Arc::new(self.locus_hash.finish())));
        cols.push(("chromosome", Arc::new(self.chromosome.finish())));
        cols.push(("start", Arc::new(self.start.finish())));
        cols.push(("end", Arc::new(self.end.finish())));
        cols.push(("reference", Arc::new(self.reference.finish())));
        cols.push(("alternate", Arc::new(self.alternate.finish())));
    }
}

fn batch(cols: Vec<(&'static str, ArrayRef)>) -> Result<RecordBatch, String> {
    let fields: Vec<Field> = cols
        .iter()
        .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        cols.into_iter().map(|(_, a)| a).collect(),
    )
    .map_err(|e| e.to_string())
}

enum ScalarBuilder {
    F32(Float32Builder),
    I32(Int32Builder),
    Bool(BooleanBuilder),
    Str(StringBuilder),
}

impl ScalarBuilder {
    fn new(col: Col) -> Self {
        match col {
            Col::F32 => Self::F32(Float32Builder::new()),
            Col::I32 => Self::I32(Int32Builder::new()),
            Col::Bool => Self::Bool(BooleanBuilder::new()),
            Col::Str => Self::Str(StringBuilder::new()),
        }
    }

    fn append(&mut self, v: &Scalar) {
        match (self, v) {
            (Self::F32(b), Scalar::F32(v)) => b.append_option(*v),
            (Self::I32(b), Scalar::I32(v)) => b.append_option(*v),
            (Self::Bool(b), Scalar::Bool(v)) => b.append_option(*v),
            (Self::Str(b), Scalar::Str(v)) => b.append_option(v.as_deref()),
            _ => unreachable!("column type fixed at construction"),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::F32(b) => Arc::new(b.finish()),
            Self::I32(b) => Arc::new(b.finish()),
            Self::Bool(b) => Arc::new(b.finish()),
            Self::Str(b) => Arc::new(b.finish()),
        }
    }
}

/// The parent columns (`father_*` / `mother_*`) of the occurrence table.
struct ParentCols {
    prefix: &'static str,
    dp: Int32Builder,
    gq: Int32Builder,
    ad_ref: Int32Builder,
    ad_alt: Int32Builder,
    ad_total: Int32Builder,
    ad_ratio: Float32Builder,
    calls: ListBuilder<Int32Builder>,
    zygosity: StringBuilder,
}

impl ParentCols {
    fn new(prefix: &'static str) -> Self {
        Self {
            prefix,
            dp: Int32Builder::new(),
            gq: Int32Builder::new(),
            ad_ref: Int32Builder::new(),
            ad_alt: Int32Builder::new(),
            ad_total: Int32Builder::new(),
            ad_ratio: Float32Builder::new(),
            calls: ListBuilder::new(Int32Builder::new()).with_field(list_of(DataType::Int32)),
            zygosity: StringBuilder::new(),
        }
    }

    /// `parent` is None for a non-progeny row (no key) and for a progeny without that parent (`{}.get`).
    fn append(&mut self, parent: Option<&Occ>) {
        match parent {
            Some(p) => {
                self.dp.append_option(p.dp);
                self.gq.append_option(p.gq);
                self.ad_ref.append_option(p.ad_ref);
                self.ad_alt.append_option(p.ad_alt);
                self.ad_total.append_option(p.ad_total);
                self.ad_ratio.append_option(p.ad_ratio);
                self.calls.values().append_slice(&p.calls);
                self.calls.append(true);
                self.zygosity.append_value(p.zygosity);
            }
            None => {
                self.dp.append_null();
                self.gq.append_null();
                self.ad_ref.append_null();
                self.ad_alt.append_null();
                self.ad_total.append_null();
                self.ad_ratio.append_null();
                self.calls.append(false);
                self.zygosity.append_null();
            }
        }
    }

    fn finish(&mut self, cols: &mut Vec<(&'static str, ArrayRef)>) {
        let p = self.prefix == "father";
        let n = |f: &'static str, m: &'static str| if p { f } else { m };
        cols.push((n("father_dp", "mother_dp"), Arc::new(self.dp.finish())));
        cols.push((n("father_gq", "mother_gq"), Arc::new(self.gq.finish())));
        cols.push((n("father_ad_ref", "mother_ad_ref"), Arc::new(self.ad_ref.finish())));
        cols.push((n("father_ad_alt", "mother_ad_alt"), Arc::new(self.ad_alt.finish())));
        cols.push((
            n("father_ad_total", "mother_ad_total"),
            Arc::new(self.ad_total.finish()),
        ));
        cols.push((
            n("father_ad_ratio", "mother_ad_ratio"),
            Arc::new(self.ad_ratio.finish()),
        ));
        cols.push((n("father_calls", "mother_calls"), Arc::new(self.calls.finish())));
        cols.push((
            n("father_zygosity", "mother_zygosity"),
            Arc::new(self.zygosity.finish()),
        ));
    }
}

struct OccurrenceCols {
    common: CommonCols,
    part: Int32Builder,
    seq_id: Int32Builder,
    tenant_code: StringBuilder,
    aliquot: StringBuilder,
    dp: Int32Builder,
    gq: Int32Builder,
    calls: ListBuilder<Int32Builder>,
    has_alt: BooleanBuilder,
    quality: Float32Builder,
    filter: StringBuilder,
    info: Vec<ScalarBuilder>,
    zygosity: StringBuilder,
    ad_ref: Int32Builder,
    ad_alt: Int32Builder,
    ad_total: Int32Builder,
    ad_ratio: Float32Builder,
    phased: BooleanBuilder,
    parental_origin: StringBuilder,
    father: ParentCols,
    mother: ParentCols,
    transmission_mode: StringBuilder,
    other_transmission_modes: ListBuilder<StringBuilder>,
    transmission_mode_candidate: BooleanBuilder,
    parental_origin_source: StringBuilder,
    transmission_mode_source: StringBuilder,
    rows: usize,
}

impl OccurrenceCols {
    fn new() -> Self {
        Self {
            common: CommonCols::new(),
            part: Int32Builder::new(),
            seq_id: Int32Builder::new(),
            tenant_code: StringBuilder::new(),
            aliquot: StringBuilder::new(),
            dp: Int32Builder::new(),
            gq: Int32Builder::new(),
            calls: ListBuilder::new(Int32Builder::new()).with_field(list_of(DataType::Int32)),
            has_alt: BooleanBuilder::new(),
            quality: Float32Builder::new(),
            filter: StringBuilder::new(),
            info: OCC_INFO.iter().map(|(_, _, c)| ScalarBuilder::new(*c)).collect(),
            zygosity: StringBuilder::new(),
            ad_ref: Int32Builder::new(),
            ad_alt: Int32Builder::new(),
            ad_total: Int32Builder::new(),
            ad_ratio: Float32Builder::new(),
            phased: BooleanBuilder::new(),
            parental_origin: StringBuilder::new(),
            father: ParentCols::new("father"),
            mother: ParentCols::new("mother"),
            transmission_mode: StringBuilder::new(),
            other_transmission_modes: ListBuilder::new(StringBuilder::new()).with_field(list_of(DataType::Utf8)),
            transmission_mode_candidate: BooleanBuilder::new(),
            parental_origin_source: StringBuilder::new(),
            transmission_mode_source: StringBuilder::new(),
            rows: 0,
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, String> {
        let mut cols: Vec<(&'static str, ArrayRef)> = Vec::new();
        self.common.finish(&mut cols);
        cols.push(("part", Arc::new(self.part.finish())));
        cols.push(("seq_id", Arc::new(self.seq_id.finish())));
        cols.push(("tenant_code", Arc::new(self.tenant_code.finish())));
        cols.push(("aliquot", Arc::new(self.aliquot.finish())));
        cols.push(("dp", Arc::new(self.dp.finish())));
        cols.push(("gq", Arc::new(self.gq.finish())));
        cols.push(("calls", Arc::new(self.calls.finish())));
        cols.push(("has_alt", Arc::new(self.has_alt.finish())));
        cols.push(("quality", Arc::new(self.quality.finish())));
        cols.push(("filter", Arc::new(self.filter.finish())));
        for (b, (name, _, _)) in self.info.iter_mut().zip(OCC_INFO.iter()) {
            cols.push((name, b.finish()));
        }
        cols.push(("zygosity", Arc::new(self.zygosity.finish())));
        cols.push(("ad_ref", Arc::new(self.ad_ref.finish())));
        cols.push(("ad_alt", Arc::new(self.ad_alt.finish())));
        cols.push(("ad_total", Arc::new(self.ad_total.finish())));
        cols.push(("ad_ratio", Arc::new(self.ad_ratio.finish())));
        cols.push(("phased", Arc::new(self.phased.finish())));
        cols.push(("parental_origin", Arc::new(self.parental_origin.finish())));
        self.father.finish(&mut cols);
        self.mother.finish(&mut cols);
        cols.push(("transmission_mode", Arc::new(self.transmission_mode.finish())));
        cols.push((
            "other_transmission_modes",
            Arc::new(self.other_transmission_modes.finish()),
        ));
        cols.push((
            "transmission_mode_candidate",
            Arc::new(self.transmission_mode_candidate.finish()),
        ));
        cols.push(("parental_origin_source", Arc::new(self.parental_origin_source.finish())));
        cols.push((
            "transmission_mode_source",
            Arc::new(self.transmission_mode_source.finish()),
        ));
        self.rows = 0;
        batch(cols)
    }
}

struct VariantCols {
    common: CommonCols,
    variant_class: StringBuilder,
    symbol: StringBuilder,
    consequences: ListBuilder<StringBuilder>,
    vep_impact: StringBuilder,
    impact_score: Int32Builder,
    mane_select: StringBuilder,
    is_mane_select: BooleanBuilder,
    is_mane_plus: BooleanBuilder,
    is_canonical: BooleanBuilder,
    rsnumber: ListBuilder<StringBuilder>,
    hgvsg: StringBuilder,
    hgvsc: StringBuilder,
    hgvsp: StringBuilder,
    dna_change: StringBuilder,
    aa_change: StringBuilder,
    transcript_id: StringBuilder,
    pick_source: StringBuilder,
}

impl VariantCols {
    fn new() -> Self {
        Self {
            common: CommonCols::new(),
            variant_class: StringBuilder::new(),
            symbol: StringBuilder::new(),
            consequences: ListBuilder::new(StringBuilder::new()).with_field(list_of(DataType::Utf8)),
            vep_impact: StringBuilder::new(),
            impact_score: Int32Builder::new(),
            mane_select: StringBuilder::new(),
            is_mane_select: BooleanBuilder::new(),
            is_mane_plus: BooleanBuilder::new(),
            is_canonical: BooleanBuilder::new(),
            rsnumber: ListBuilder::new(StringBuilder::new()).with_field(list_of(DataType::Utf8)),
            hgvsg: StringBuilder::new(),
            hgvsc: StringBuilder::new(),
            hgvsp: StringBuilder::new(),
            dna_change: StringBuilder::new(),
            aa_change: StringBuilder::new(),
            transcript_id: StringBuilder::new(),
            pick_source: StringBuilder::new(),
        }
    }

    fn append(&mut self, task_id: i32, c: &Common, id: Option<&str>, picked: Option<&Consequence>) {
        self.common.append(task_id, c);
        // `"rsnumber": record.ID` into a list column: pyarrow reads the Python str as a sequence of characters,
        // so the Python ETL stores ["r", "s", "1", ...]. Kept identical here; see the POC notes.
        match id {
            Some(id) => {
                for ch in id.chars() {
                    let mut buf = [0u8; 4];
                    self.rsnumber.values().append_value(ch.encode_utf8(&mut buf));
                }
                self.rsnumber.append(true);
            }
            None => self.rsnumber.append(false),
        }
        match picked {
            Some(p) => {
                self.variant_class.append_option(p.variant_class);
                self.symbol.append_option(p.symbol);
                for s in &p.consequences {
                    self.consequences.values().append_value(s);
                }
                self.consequences.append(true);
                self.vep_impact.append_option(p.vep_impact);
                self.impact_score.append_value(p.impact_score);
                self.mane_select.append_option(p.mane_select);
                self.is_mane_select.append_value(p.is_mane_select);
                self.is_mane_plus.append_value(p.is_mane_plus);
                self.is_canonical.append_value(p.is_canonical);
                self.hgvsg.append_option(p.hgvsg);
                self.hgvsc.append_option(p.hgvsc);
                self.hgvsp.append_option(p.hgvsp);
                self.dna_change.append_option(p.dna_change);
                self.aa_change.append_option(p.aa_change);
                self.transcript_id.append_option(p.transcript_id);
                self.pick_source.append_option(p.source);
            }
            None => {
                self.variant_class.append_null();
                self.symbol.append_null();
                self.consequences.append(false);
                self.vep_impact.append_null();
                self.impact_score.append_null();
                self.mane_select.append_null();
                self.is_mane_select.append_null();
                self.is_mane_plus.append_null();
                self.is_canonical.append_null();
                self.hgvsg.append_null();
                self.hgvsc.append_null();
                self.hgvsp.append_null();
                self.dna_change.append_null();
                self.aa_change.append_null();
                self.transcript_id.append_null();
                self.pick_source.append_null();
            }
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, String> {
        let mut cols: Vec<(&'static str, ArrayRef)> = Vec::new();
        self.common.finish(&mut cols);
        cols.push(("variant_class", Arc::new(self.variant_class.finish())));
        cols.push(("symbol", Arc::new(self.symbol.finish())));
        cols.push(("consequences", Arc::new(self.consequences.finish())));
        cols.push(("vep_impact", Arc::new(self.vep_impact.finish())));
        cols.push(("impact_score", Arc::new(self.impact_score.finish())));
        cols.push(("mane_select", Arc::new(self.mane_select.finish())));
        cols.push(("is_mane_select", Arc::new(self.is_mane_select.finish())));
        cols.push(("is_mane_plus", Arc::new(self.is_mane_plus.finish())));
        cols.push(("is_canonical", Arc::new(self.is_canonical.finish())));
        cols.push(("rsnumber", Arc::new(self.rsnumber.finish())));
        cols.push(("hgvsg", Arc::new(self.hgvsg.finish())));
        cols.push(("hgvsc", Arc::new(self.hgvsc.finish())));
        cols.push(("hgvsp", Arc::new(self.hgvsp.finish())));
        cols.push(("dna_change", Arc::new(self.dna_change.finish())));
        cols.push(("aa_change", Arc::new(self.aa_change.finish())));
        cols.push(("transcript_id", Arc::new(self.transcript_id.finish())));
        cols.push(("pick_source", Arc::new(self.pick_source.finish())));
        batch(cols)
    }
}

struct ConsequenceCols {
    common: CommonCols,
    variant_class: StringBuilder,
    hgvsg: StringBuilder,
    hgvsp: StringBuilder,
    hgvsc: StringBuilder,
    symbol: StringBuilder,
    transcript_id: StringBuilder,
    source: StringBuilder,
    biotype: StringBuilder,
    strand: StringBuilder,
    exon: StructBuilder,
    vep_impact: StringBuilder,
    consequences: ListBuilder<StringBuilder>,
    mane_select: StringBuilder,
    is_mane_select: BooleanBuilder,
    is_mane_plus: BooleanBuilder,
    is_picked: BooleanBuilder,
    is_canonical: BooleanBuilder,
    aa_change: StringBuilder,
    dna_change: StringBuilder,
    impact_score: Int32Builder,
    mane_pair_transcript_id: StringBuilder,
    transcript_version: StringBuilder,
    cds_start: Int32Builder,
    cds_end: Int32Builder,
}

fn exon_fields() -> Fields {
    Fields::from(vec![
        Field::new("rank", DataType::Utf8, true),
        Field::new("total", DataType::Utf8, true),
    ])
}

impl ConsequenceCols {
    fn new() -> Self {
        Self {
            common: CommonCols::new(),
            variant_class: StringBuilder::new(),
            hgvsg: StringBuilder::new(),
            hgvsp: StringBuilder::new(),
            hgvsc: StringBuilder::new(),
            symbol: StringBuilder::new(),
            transcript_id: StringBuilder::new(),
            source: StringBuilder::new(),
            biotype: StringBuilder::new(),
            strand: StringBuilder::new(),
            exon: StructBuilder::from_fields(exon_fields(), 0),
            vep_impact: StringBuilder::new(),
            consequences: ListBuilder::new(StringBuilder::new()).with_field(list_of(DataType::Utf8)),
            mane_select: StringBuilder::new(),
            is_mane_select: BooleanBuilder::new(),
            is_mane_plus: BooleanBuilder::new(),
            is_picked: BooleanBuilder::new(),
            is_canonical: BooleanBuilder::new(),
            aa_change: StringBuilder::new(),
            dna_change: StringBuilder::new(),
            impact_score: Int32Builder::new(),
            mane_pair_transcript_id: StringBuilder::new(),
            transcript_version: StringBuilder::new(),
            cds_start: Int32Builder::new(),
            cds_end: Int32Builder::new(),
        }
    }

    fn append(&mut self, task_id: i32, c: &Common, q: &Consequence) {
        self.common.append(task_id, c);
        self.variant_class.append_option(q.variant_class);
        self.hgvsg.append_option(q.hgvsg);
        self.hgvsp.append_option(q.hgvsp);
        self.hgvsc.append_option(q.hgvsc);
        self.symbol.append_option(q.symbol);
        self.transcript_id.append_option(q.transcript_id);
        self.source.append_option(q.source);
        self.biotype.append_option(q.biotype);
        self.strand.append_option(q.strand);
        match &q.exon {
            Some((rank, total)) => {
                self.exon.field_builder::<StringBuilder>(0).unwrap().append_value(rank);
                self.exon.field_builder::<StringBuilder>(1).unwrap().append_value(total);
                self.exon.append(true);
            }
            None => {
                self.exon.field_builder::<StringBuilder>(0).unwrap().append_null();
                self.exon.field_builder::<StringBuilder>(1).unwrap().append_null();
                self.exon.append(false);
            }
        }
        self.vep_impact.append_option(q.vep_impact);
        for s in &q.consequences {
            self.consequences.values().append_value(s);
        }
        self.consequences.append(true);
        self.mane_select.append_option(q.mane_select);
        self.is_mane_select.append_value(q.is_mane_select);
        self.is_mane_plus.append_value(q.is_mane_plus);
        self.is_picked.append_value(q.is_picked);
        self.is_canonical.append_value(q.is_canonical);
        self.aa_change.append_option(q.aa_change);
        self.dna_change.append_option(q.dna_change);
        self.impact_score.append_value(q.impact_score);
        self.mane_pair_transcript_id.append_option(q.mane_pair_transcript_id);
        self.transcript_version.append_option(q.transcript_version);
        self.cds_start.append_option(q.cds_start);
        self.cds_end.append_option(q.cds_end);
    }

    fn finish(&mut self) -> Result<RecordBatch, String> {
        let mut cols: Vec<(&'static str, ArrayRef)> = Vec::new();
        self.common.finish(&mut cols);
        cols.push(("variant_class", Arc::new(self.variant_class.finish())));
        cols.push(("hgvsg", Arc::new(self.hgvsg.finish())));
        cols.push(("hgvsp", Arc::new(self.hgvsp.finish())));
        cols.push(("hgvsc", Arc::new(self.hgvsc.finish())));
        cols.push(("symbol", Arc::new(self.symbol.finish())));
        cols.push(("transcript_id", Arc::new(self.transcript_id.finish())));
        cols.push(("source", Arc::new(self.source.finish())));
        cols.push(("biotype", Arc::new(self.biotype.finish())));
        cols.push(("strand", Arc::new(self.strand.finish())));
        cols.push(("exon", Arc::new(self.exon.finish())));
        cols.push(("vep_impact", Arc::new(self.vep_impact.finish())));
        cols.push(("consequences", Arc::new(self.consequences.finish())));
        cols.push(("mane_select", Arc::new(self.mane_select.finish())));
        cols.push(("is_mane_select", Arc::new(self.is_mane_select.finish())));
        cols.push(("is_mane_plus", Arc::new(self.is_mane_plus.finish())));
        cols.push(("is_picked", Arc::new(self.is_picked.finish())));
        cols.push(("is_canonical", Arc::new(self.is_canonical.finish())));
        cols.push(("aa_change", Arc::new(self.aa_change.finish())));
        cols.push(("dna_change", Arc::new(self.dna_change.finish())));
        cols.push(("impact_score", Arc::new(self.impact_score.finish())));
        cols.push((
            "mane_pair_transcript_id",
            Arc::new(self.mane_pair_transcript_id.finish()),
        ));
        cols.push(("transcript_version", Arc::new(self.transcript_version.finish())));
        cols.push(("cds_start", Arc::new(self.cds_start.finish())));
        cols.push(("cds_end", Arc::new(self.cds_end.finish())));
        batch(cols)
    }
}

struct ComphetCols {
    task_id: Int32Builder,
    locus_hash: StringBuilder,
    part: Int32Builder,
    seq_id: Int32Builder,
    tenant_code: StringBuilder,
    partner_locus_hash: StringBuilder,
    symbol: StringBuilder,
}

impl ComphetCols {
    fn new() -> Self {
        Self {
            task_id: Int32Builder::new(),
            locus_hash: StringBuilder::new(),
            part: Int32Builder::new(),
            seq_id: Int32Builder::new(),
            tenant_code: StringBuilder::new(),
            partner_locus_hash: StringBuilder::new(),
            symbol: StringBuilder::new(),
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, String> {
        batch(vec![
            ("task_id", Arc::new(self.task_id.finish())),
            ("locus_hash", Arc::new(self.locus_hash.finish())),
            ("part", Arc::new(self.part.finish())),
            ("seq_id", Arc::new(self.seq_id.finish())),
            ("tenant_code", Arc::new(self.tenant_code.finish())),
            ("partner_locus_hash", Arc::new(self.partner_locus_hash.finish())),
            ("symbol", Arc::new(self.symbol.finish())),
        ])
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------------------------------------------

/// Counters the Python side logs at the end of a file.
#[derive(Default)]
pub struct Stats {
    pub records: u64,
    pub kept: u64,
    pub skipped_no_alt: u64,
    pub skipped_multi_allelic: u64,
    pub skipped_unsupported_chromosome: u64,
    pub csq: CsqStats,
    pub malformed_comphet: u64,
    /// The first malformed `slivar_comphet` entries, as `(entry, locus)`.
    pub malformed_comphet_examples: Vec<(String, String)>,
}

pub struct Batches {
    pub occurrence: RecordBatch,
    pub variant: RecordBatch,
    pub consequence: RecordBatch,
    pub comphet: RecordBatch,
}

pub struct GermlineReader {
    reader: bcf::Reader,
    record: bcf::Record,
    hdr: *mut htslib::bcf_hdr_t,
    task_id: i32,
    part: i32,
    pub samples: Vec<String>,
    ped: Pedigree,
    pub slivar_parental_origin: bool,
    pub slivar_transmission_mode: bool,
    csq_idx: CsqIndexes,
    csq_key: InfoKey,
    comphet_key: Option<InfoKey>,
    occ_info: Vec<Option<InfoKey>>,
    slivar_keys: Vec<(&'static str, InfoKey)>,
    gt_buf: I32Buf,
    ad_buf: I32Buf,
    ao_buf: I32Buf,
    dp_buf: I32Buf,
    gq_buf: I32Buf,
    tag_gt: CString,
    tag_ad: CString,
    tag_ro: CString,
    tag_ao: CString,
    tag_dp: CString,
    tag_gq: CString,
    occ: OccurrenceCols,
    var: VariantCols,
    csq: ConsequenceCols,
    comphet: ComphetCols,
    pub stats: Stats,
    done: bool,
}

// The raw pointers are owned by `reader` / the buffers and only touched by the one thread holding `&mut self`.
unsafe impl Send for GermlineReader {}

impl GermlineReader {
    pub fn open(
        path: &str,
        experiments: Vec<Experiment>,
        task_id: i32,
        part: i32,
        threads: Option<usize>,
    ) -> Result<Self, String> {
        let mut reader = bcf::Reader::from_path(path).map_err(|e| format!("cannot open {path}: {e}"))?;
        if let Some(n) = threads.filter(|n| *n > 0) {
            reader.set_threads(n).map_err(|e| e.to_string())?;
        }
        let hdr = unsafe { reader.header().as_ptr() };

        // cyvcf2 `VCF(samples=[...])`: `bcf_hdr_set_samples` with the comma-joined aliquots. A positive return is
        // the index of the first aliquot the file lacks: cyvcf2 warns and goes on with the others.
        let wanted = cstring(
            &experiments
                .iter()
                .map(|e| e.aliquot.as_str())
                .collect::<Vec<_>>()
                .join(","),
        );
        let ret = unsafe { htslib::bcf_hdr_set_samples(hdr, wanted.as_ptr(), 0) };
        if ret < 0 {
            return Err(format!(
                "cannot select samples in {path} (bcf_hdr_set_samples returned {ret})"
            ));
        }
        let samples: Vec<String> = reader
            .header()
            .samples()
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect();
        if samples.is_empty() {
            return Err(format!("Task {task_id} has no matching samples in the VCF file {path}"));
        }

        // INFO header lines: IDs (original spelling), types and the CSQ description.
        let mut info_ids: Vec<String> = Vec::new();
        let mut info_flag: HashMap<String, bool> = HashMap::new();
        let mut csq_description: Option<String> = None;
        let mut csq_in_format: Option<String> = None;
        for rec in reader.header().header_records() {
            match rec {
                HeaderRecord::Info { values, .. } => {
                    if let Some(id) = values.get("ID") {
                        info_flag.insert(id.clone(), values.get("Type").is_some_and(|t| t == "Flag"));
                        if id == "CSQ" {
                            csq_description = Some(values.get("Description").cloned().unwrap_or_default());
                        }
                        info_ids.push(id.clone());
                    }
                }
                HeaderRecord::Format { values, .. } if values.get("ID").is_some_and(|id| id == "CSQ") => {
                    csq_in_format = Some(values.get("Description").cloned().unwrap_or_default());
                }
                _ => {}
            }
        }
        // `vcf.get_header_type("CSQ")` raises KeyError when no INFO (nor FORMAT) line declares it.
        let description = csq_description
            .or(csq_in_format)
            .ok_or_else(|| "KeyError: b'CSQ' (no CSQ header line)".to_string())?;
        let csq_idx = consequence::parse_csq_header(&description);

        // `InfoKeys`: lowercased header ID -> header ID, the last line winning.
        let mut lower: HashMap<String, String> = HashMap::new();
        for id in &info_ids {
            lower.insert(id.to_lowercase(), id.clone());
        }
        let key_for = |id: &str| InfoKey {
            key: cstring(id),
            is_flag: *info_flag.get(id).unwrap_or(&false),
        };
        let resolve = |name: &str| -> Option<InfoKey> {
            std::iter::once(name)
                .chain(info_aliases(name).iter().copied())
                .find_map(|c| lower.get(&c.to_lowercase()))
                .map(|id| key_for(id))
        };
        let occ_info = OCC_INFO.iter().map(|(_, name, _)| resolve(name)).collect();

        let id_set: HashSet<&str> = info_ids.iter().map(String::as_str).collect();
        let slivar_parental_origin = inheritance::PO_TAGS.iter().any(|(t, _)| id_set.contains(t));
        let slivar_transmission_mode = id_set.contains(inheritance::MODE_TAGS_MARKER);
        let slivar_keys = inheritance::PO_TAGS
            .iter()
            .map(|(t, _)| *t)
            .chain(inheritance::MODE_TAGS.iter().map(|(t, _)| *t))
            .chain(inheritance::GENOTYPE_TAGS.iter().copied())
            .chain(std::iter::once(inheritance::CANDIDATE_TAG))
            .map(|t| (t, key_for(t)))
            .collect();

        let ped = Pedigree::new(&experiments, &samples);
        // Python keys the occurrences of a record by seq_id: a repeated seq_id would silently merge two samples.
        // A task's experiments have distinct seq_ids, so refuse the case rather than port the merge.
        let mut seen = HashSet::new();
        if !ped.experiments.iter().all(|e| seen.insert(e.seq_id)) {
            return Err(format!("Task {task_id}: two experiments share a seq_id"));
        }
        let record = reader.empty_record();
        Ok(Self {
            reader,
            record,
            hdr,
            task_id,
            part,
            samples,
            ped,
            slivar_parental_origin,
            slivar_transmission_mode,
            csq_idx,
            csq_key: key_for("CSQ"),
            comphet_key: Some(key_for("slivar_comphet")),
            occ_info,
            slivar_keys,
            gt_buf: I32Buf::new(),
            ad_buf: I32Buf::new(),
            ao_buf: I32Buf::new(),
            dp_buf: I32Buf::new(),
            gq_buf: I32Buf::new(),
            tag_gt: cstring("GT"),
            tag_ad: cstring("AD"),
            tag_ro: cstring("RO"),
            tag_ao: cstring("AO"),
            tag_dp: cstring("DP"),
            tag_gq: cstring("GQ"),
            occ: OccurrenceCols::new(),
            var: VariantCols::new(),
            csq: ConsequenceCols::new(),
            comphet: ComphetCols::new(),
            stats: Stats::default(),
            done: false,
        })
    }

    /// Reads the whole file into one Parquet sink per table (`occurrence`, `variant`, `consequence`, `comphet`),
    /// `batch_records` kept records at a time. Returns the files of each table.
    pub fn write_parquet(
        &mut self,
        sinks: &mut [(String, crate::parquet_sink::ParquetSink)],
        batch_records: usize,
    ) -> Result<Vec<(String, Vec<crate::parquet_sink::WrittenFile>)>, String> {
        let sink = |sinks: &mut [(String, crate::parquet_sink::ParquetSink)], key: &str| {
            sinks
                .iter()
                .position(|(k, _)| k == key)
                .ok_or_else(|| format!("no Parquet sink for table {key}"))
        };
        let idx = [
            sink(sinks, "occurrence")?,
            sink(sinks, "variant")?,
            sink(sinks, "consequence")?,
            sink(sinks, "comphet")?,
        ];
        while let Some(b) = self.next_batches(batch_records)? {
            for (i, batch) in idx.iter().zip([&b.occurrence, &b.variant, &b.consequence, &b.comphet]) {
                let (key, s) = &mut sinks[*i];
                s.write(batch).map_err(|e| format!("table {key}: {e}"))?;
            }
        }
        sinks.iter_mut().map(|(k, s)| Ok((k.clone(), s.finish()?))).collect()
    }

    /// Reads until `max_records` records are kept or the file ends. None once the file is exhausted.
    pub fn next_batches(&mut self, max_records: usize) -> Result<Option<Batches>, String> {
        if self.done {
            return Ok(None);
        }
        let mut kept = 0usize;
        while kept < max_records {
            match self.reader.read(&mut self.record) {
                None => {
                    self.done = true;
                    break;
                }
                Some(Err(e)) => return Err(format!("error reading VCF: {e}")),
                Some(Ok(())) => {}
            }
            self.stats.records += 1;
            if self.process_record()? {
                kept += 1;
            }
        }
        Ok(Some(Batches {
            occurrence: self.occ.finish()?,
            variant: self.var.finish()?,
            consequence: self.csq.finish()?,
            comphet: self.comphet.finish()?,
        }))
    }

    /// One record. True when it reached the tables.
    fn process_record(&mut self) -> Result<bool, String> {
        let hdr = self.hdr;
        let rec_ptr: *mut htslib::bcf1_t = self.record.inner_mut();
        let rec: &htslib::bcf1_t = unsafe { &*rec_ptr };

        let alleles = hts::alleles(rec);
        if alleles.len() > 2 {
            self.stats.skipped_multi_allelic += 1;
            return Ok(false);
        }
        let chrom_raw = match u32::try_from(rec.rid)
            .ok()
            .and_then(|rid| self.reader.header().rid2name(rid).ok())
        {
            Some(name) => String::from_utf8_lossy(name).into_owned(),
            None => return Err(format!("record with unknown contig id {}", rec.rid)),
        };
        if !SUPPORTED_CHROMOSOMES.contains(&chrom_raw.as_ref()) {
            self.stats.skipped_unsupported_chromosome += 1;
            return Ok(false);
        }

        // --- process_common
        let reference = alleles.first().cloned().unwrap_or_default();
        let alternate = alleles.get(1).cloned().ok_or_else(|| {
            format!(
                "IndexError: list index out of range (no ALT at {chrom_raw}:{})",
                rec.pos + 1
            )
        })?;
        let chromosome = chrom_raw.replace("chr", "");
        let pos = rec.pos + 1;
        let (locus, locus_hash) = locus_and_hash(&chromosome, pos, &reference, &alternate);
        let common = Common {
            locus,
            locus_hash,
            start: i32::try_from(pos).map_err(|_| "POS overflows int32")?,
            end: i32::try_from(rec.pos + rec.rlen).map_err(|_| "END overflows int32")?,
            chromosome,
            reference,
            alternate,
        };

        // --- process_occurrence
        let n = self.samples.len();
        let ngts = unsafe { self.gt_buf.get_format_int32(hdr, rec_ptr, &self.tag_gt) };
        if ngts < 0 {
            return Err(format!(
                "error parsing genotypes; they may be absent from this record ({})",
                common.locus
            ));
        }
        let nper = ngts as usize / n;
        if nper == 0 {
            return Err(format!("IndexError: record without genotype values ({})", common.locus));
        }
        let gts = self.gt_buf.values(ngts);

        let nad = unsafe { self.ad_buf.get_format_int32(hdr, rec_ptr, &self.tag_ad) };
        let depths = if nad > 0 {
            genotype::ref_alt_depths(DepthSource::Ad(self.ad_buf.values(nad)), n)?
        } else if nad == -1 {
            // Freebayes: RO / AO. `ad_buf` is free to take RO now.
            let nro = unsafe { self.ad_buf.get_format_int32(hdr, rec_ptr, &self.tag_ro) };
            let nao = unsafe { self.ao_buf.get_format_int32(hdr, rec_ptr, &self.tag_ao) };
            let ro = (nro >= 0).then(|| self.ad_buf.values(nro));
            let ao = (nao >= 0).then(|| self.ao_buf.values(nao));
            genotype::ref_alt_depths(DepthSource::RoAo(ro, ao), n)?
        } else {
            genotype::ref_alt_depths(DepthSource::Unknown, n)?
        };
        let (ref_depths, alt_depths) = depths;

        let dp = Self::first_format_int(hdr, rec_ptr, &self.tag_dp, &mut self.dp_buf, n)?;
        let gq = Self::first_format_int(hdr, rec_ptr, &self.tag_gq, &mut self.gq_buf, n)?;

        let mut occs: Vec<Occ> = Vec::with_capacity(self.ped.experiments.len());
        for idx in 0..self.ped.experiments.len() {
            let r = ref_depths[idx];
            let a = alt_depths[idx];
            let ad_ref = (r > 0).then_some(r);
            let ad_alt = (a > 0).then_some(a);
            let zyg = genotype::gt_type(gts, nper, idx);
            let mut calls = genotype::calls(gts, nper, idx);
            let zygosity: &'static str = if (ad_alt.is_some_and(|a| a < 3)
                && (zyg == genotype::HET || zyg == genotype::HOM_ALT))
                || (ad_ref.is_some_and(|r| r < 3) && zyg == genotype::HOM_REF)
            {
                calls = vec![-1; calls.len()];
                "UNK"
            } else if zyg == genotype::HOM_ALT && calls.len() == 1 {
                "HEM"
            } else {
                match zyg {
                    genotype::HOM_REF => "WT",
                    genotype::HET => "HET",
                    genotype::HOM_ALT => "HOM",
                    _ => "UNK",
                }
            };
            let total = genotype::depth(r, a);
            let freq = genotype::alt_freq(total, a);
            let dp_v = dp.as_ref().map_or(0, |v| v[idx]);
            let gq_v = gq.as_ref().map_or(0, |v| v[idx]);
            occs.push(Occ {
                dp: (dp_v > 0).then_some(dp_v),
                gq: (gq_v > 0).then_some(gq_v),
                has_alt: calls.contains(&1),
                calls,
                zygosity,
                ad_ref,
                ad_alt,
                ad_total: (total > 0).then_some(total),
                ad_ratio: (freq > 0.0).then_some(freq as f32),
                phased: genotype::phased(gts, nper, idx),
            });
        }

        // RAD-75: a record no sample carries is dropped from all the tables together.
        if !occs.iter().any(|o| o.has_alt) {
            self.stats.skipped_no_alt += 1;
            return Ok(false);
        }

        let family = self.family_columns(rec_ptr, &common, &occs);

        // INFO values shared by all the samples of the record.
        let info: Vec<Scalar> = OCC_INFO
            .iter()
            .zip(self.occ_info.iter())
            .map(|((_, name, col), key)| {
                let v = key
                    .as_ref()
                    .and_then(|k| unsafe { hts::get_info(hdr, rec_ptr, &k.key, k.is_flag) });
                to_column(v, *col, name)
            })
            .collect::<Result<_, _>>()?;
        let quality = hts::qual(rec).map(|q| q.trunc());
        let filter = unsafe { hts::filter(hdr, rec) }.unwrap_or_else(|| "PASS".to_string());

        let father_idx = self
            .ped
            .father_seq_id
            .and_then(|s| self.ped.experiments.iter().position(|e| e.seq_id == s));
        let mother_idx = self
            .ped
            .mother_seq_id
            .and_then(|s| self.ped.experiments.iter().position(|e| e.seq_id == s));

        let o = &mut self.occ;
        for (idx, (exp, occ)) in self.ped.experiments.iter().zip(occs.iter()).enumerate() {
            o.common.append(self.task_id, &common);
            o.part.append_value(self.part);
            o.seq_id.append_value(exp.seq_id);
            o.tenant_code.append_value(&exp.tenant_code);
            o.aliquot.append_value(&exp.aliquot);
            o.dp.append_option(occ.dp);
            o.gq.append_option(occ.gq);
            o.calls.values().append_slice(&occ.calls);
            o.calls.append(true);
            o.has_alt.append_value(occ.has_alt);
            o.quality.append_option(quality);
            o.filter.append_value(&filter);
            for (b, v) in o.info.iter_mut().zip(info.iter()) {
                b.append(v);
            }
            o.zygosity.append_value(occ.zygosity);
            o.ad_ref.append_option(occ.ad_ref);
            o.ad_alt.append_option(occ.ad_alt);
            o.ad_total.append_option(occ.ad_total);
            o.ad_ratio.append_option(occ.ad_ratio);
            o.phased.append_value(occ.phased);
            match family.get(&idx) {
                Some(f) => {
                    o.parental_origin.append_option(f.parental_origin);
                    o.father.append(father_idx.map(|i| &occs[i]));
                    o.mother.append(mother_idx.map(|i| &occs[i]));
                    o.transmission_mode.append_option(f.transmission_mode);
                    match &f.other_transmission_modes {
                        Some(m) => {
                            for s in m {
                                o.other_transmission_modes.values().append_value(s);
                            }
                            o.other_transmission_modes.append(true);
                        }
                        None => o.other_transmission_modes.append(false),
                    }
                    o.transmission_mode_candidate
                        .append_option(f.transmission_mode_candidate);
                    o.parental_origin_source.append_option(f.parental_origin_source);
                    o.transmission_mode_source.append_option(f.transmission_mode_source);
                }
                None => {
                    o.parental_origin.append_null();
                    o.father.append(None);
                    o.mother.append(None);
                    o.transmission_mode.append_null();
                    o.other_transmission_modes.append(false);
                    o.transmission_mode_candidate.append_null();
                    o.parental_origin_source.append_null();
                    o.transmission_mode_source.append_null();
                }
            }
            o.rows += 1;
        }

        // --- process_consequence + process_variant
        let csq_value = unsafe { hts::get_info(hdr, rec_ptr, &self.csq_key.key, self.csq_key.is_flag) };
        let csq_str = match &csq_value {
            None => None,
            Some(InfoValue::Str(s)) => Some(s.as_str()),
            Some(other) => return Err(format!("INFO/CSQ is not a string: {other:?}")),
        };
        let (picked, consequences) = consequence::process_consequence(csq_str, &self.csq_idx, &mut self.stats.csq)?;
        for q in &consequences {
            self.csq.append(self.task_id, &common, q);
        }
        let id = hts::id(rec);
        self.var
            .append(self.task_id, &common, id.as_deref(), picked.map(|i| &consequences[i]));

        // --- process_comphet
        self.comphet_rows(rec_ptr, &common)?;

        self.stats.kept += 1;
        Ok(true)
    }

    /// `record.format(tag)[:, 0]` when the record carries the tag, else None (every sample reads 0).
    fn first_format_int(
        hdr: *mut htslib::bcf_hdr_t,
        rec: *mut htslib::bcf1_t,
        tag: &CString,
        buf: &mut I32Buf,
        n: usize,
    ) -> Result<Option<Vec<i32>>, String> {
        if !unsafe { hts::has_format(hdr, rec, tag) } {
            return Ok(None);
        }
        let nret = unsafe { buf.get_format_int32(hdr, rec, tag) };
        if nret < 0 {
            return Err(format!(
                "FORMAT/{} is not an Integer field (htslib {nret}); the Python ETL does not support it either",
                tag.to_string_lossy()
            ));
        }
        let nper = nret as usize / n;
        let v = buf.values(nret);
        Ok(Some((0..n).map(|i| v[i * nper]).collect()))
    }

    /// The family columns of each progeny, keyed by its index in `ped.experiments`.
    fn family_columns(&self, rec: *mut htslib::bcf1_t, common: &Common, occs: &[Occ]) -> HashMap<usize, Family> {
        let mut out = HashMap::new();
        if !self.ped.is_family {
            return out;
        }
        let pos = |s: Option<i32>| s.and_then(|s| self.ped.experiments.iter().position(|e| e.seq_id == s));
        let father = pos(self.ped.father_seq_id).map(|i| inheritance::normalize_calls(&occs[i].calls));
        let mother = pos(self.ped.mother_seq_id).map(|i| inheritance::normalize_calls(&occs[i].calls));
        let slivar = (self.slivar_parental_origin || self.slivar_transmission_mode).then(|| self.slivar_tags(rec));
        for &p in &self.ped.progenies {
            let exp = &self.ped.experiments[p];
            let progeny = inheritance::normalize_calls(&occs[p].calls);
            let mut f = Family::default();
            if self.slivar_parental_origin {
                f.parental_origin = slivar.as_ref().unwrap().parental_origin(&exp.aliquot);
                f.parental_origin_source = Some("slivar");
            } else {
                f.parental_origin = Some(inheritance::parental_origin(
                    &common.chromosome,
                    &progeny,
                    father.as_deref(),
                    mother.as_deref(),
                ));
                f.parental_origin_source = Some("etl");
            }
            if self.slivar_transmission_mode {
                let (mode, others, candidate) = slivar.as_ref().unwrap().transmission_mode(&exp.aliquot);
                f.transmission_mode = mode;
                f.other_transmission_modes = Some(others);
                f.transmission_mode_candidate = Some(candidate);
                f.transmission_mode_source = Some("slivar");
            } else {
                f.transmission_mode = inheritance::transmission_mode(
                    &common.chromosome,
                    &exp.sex,
                    &progeny,
                    father.as_deref(),
                    mother.as_deref(),
                    self.ped.is_father_affected,
                    self.ped.is_mother_affected,
                );
                f.transmission_mode_source = Some("etl");
            }
            out.insert(p, f);
        }
        out
    }

    fn slivar_tags(&self, rec: *mut htslib::bcf1_t) -> SlivarTags {
        let mut listed = HashMap::new();
        for (tag, key) in &self.slivar_keys {
            if let Some(v) = unsafe { hts::get_info(self.hdr, rec, &key.key, key.is_flag) }
                && let Some(s) = v.as_py_str()
            {
                listed.insert(*tag, s.split(',').map(str::to_string).collect());
            }
        }
        SlivarTags { listed }
    }

    fn comphet_rows(&mut self, rec: *mut htslib::bcf1_t, common: &Common) -> Result<(), String> {
        let Some(key) = &self.comphet_key else {
            return Ok(());
        };
        let value = match unsafe { hts::get_info(self.hdr, rec, &key.key, key.is_flag) } {
            None => return Ok(()),
            Some(InfoValue::Str(s)) if s.is_empty() => return Ok(()),
            Some(InfoValue::Str(s)) => s,
            Some(InfoValue::Bool(false)) => return Ok(()),
            Some(other) => return Err(format!("INFO/slivar_comphet is not a string: {other:?}")),
        };
        let mut seen: HashSet<(i32, String, String)> = HashSet::new();
        for entry in value.split(',') {
            let fields: Vec<&str> = entry.split('/').collect();
            if fields.len() != 7 || fields[1].is_empty() || !is_py_digit(fields[4]) {
                self.stats.malformed_comphet += 1;
                if self.stats.malformed_comphet_examples.len() < 100 {
                    self.stats
                        .malformed_comphet_examples
                        .push((entry.to_string(), common.locus.clone()));
                }
                continue;
            }
            let (sample, symbol, chrom, pos, r, a) =
                (fields[0], fields[1], fields[3], fields[4], fields[5], fields[6]);
            let Some(exp) = self
                .ped
                .progenies
                .iter()
                .map(|&i| &self.ped.experiments[i])
                .rfind(|e| e.aliquot == sample)
            else {
                continue;
            };
            let pos: i64 = pos
                .parse()
                .map_err(|_| format!("slivar_comphet position {pos} out of range"))?;
            let (_, partner) = locus_and_hash(&chrom.replace("chr", ""), pos, r, a);
            if !seen.insert((exp.seq_id, partner.clone(), symbol.to_string())) {
                continue;
            }
            self.comphet.task_id.append_value(self.task_id);
            self.comphet.locus_hash.append_value(&common.locus_hash);
            self.comphet.part.append_value(self.part);
            self.comphet.seq_id.append_value(exp.seq_id);
            self.comphet.tenant_code.append_value(&exp.tenant_code);
            self.comphet.partner_locus_hash.append_value(&partner);
            self.comphet.symbol.append_value(symbol);
        }
        Ok(())
    }
}

/// `str.isdigit()` for the ASCII digits a VCF position holds.
fn is_py_digit(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit())
}

impl Stats {
    /// The scalar counters, keyed by name.
    pub fn counts(&self) -> BTreeMap<&'static str, u64> {
        let s = self;
        let mut m = BTreeMap::new();
        m.insert("records", s.records);
        m.insert("kept", s.kept);
        m.insert("skipped_no_alt", s.skipped_no_alt);
        m.insert("skipped_multi_allelic", s.skipped_multi_allelic);
        m.insert("skipped_unsupported_chromosome", s.skipped_unsupported_chromosome);
        m.insert("unclassified_sources", s.csq.unclassified);
        m.insert("malformed_comphet", s.malformed_comphet);
        m
    }
}
