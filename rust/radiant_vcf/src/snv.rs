//! What the germline and somatic SNV extractions share: port of `radiant/tasks/vcf/snv/common.py`, `variant.py`,
//! the header half of `consequence.py`, `vcf_utils.InfoKeys`, and the record checks of both `process.py`.

use std::collections::{BTreeMap, HashMap};
use std::ffi::{CStr, CString};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Int32Builder, ListBuilder, StringBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use rust_htslib::bcf::header::{HeaderRecord, HeaderView};
use rust_htslib::htslib;
use sha2::{Digest, Sha256};

use crate::consequence::{self, Consequence, CsqIndexes, CsqStats};
use crate::genotype::{self, DepthSource};
use crate::hts::{self, F32Buf, I32Buf, InfoValue, cstring};

pub const SUPPORTED_CHROMOSOMES: [&str; 25] = [
    "chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11", "chr12", "chr13",
    "chr14", "chr15", "chr16", "chr17", "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY", "chrM",
];

// ---------------------------------------------------------------------------------------------------------------
// INFO
// ---------------------------------------------------------------------------------------------------------------

/// The type of an occurrence column that holds an INFO value.
#[derive(Clone, Copy)]
pub enum Col {
    F32,
    I32,
    Bool,
    Str,
}

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
pub struct InfoKey {
    pub key: CString,
    pub is_flag: bool,
}

impl InfoKey {
    /// `record.INFO.get(key)`.
    ///
    /// # Safety
    /// `hdr` and `rec` must be valid, `rec` unpacked.
    pub unsafe fn get(&self, hdr: *mut htslib::bcf_hdr_t, rec: *mut htslib::bcf1_t) -> Option<InfoValue> {
        unsafe { hts::get_info(hdr, rec, &self.key, self.is_flag) }
    }
}

/// The value of one INFO field in a typed column.
#[derive(Clone)]
pub enum Scalar {
    F32(Option<f32>),
    I32(Option<i32>),
    Bool(Option<bool>),
    Str(Option<String>),
}

/// A Python INFO value in a typed column, or the error pyarrow raises for it.
pub fn to_column(v: Option<InfoValue>, col: Col, name: &str) -> Result<Scalar, String> {
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

/// The INFO columns of an occurrence table: `(column, name the ETL reads it under, type)`, and the header key each
/// name resolves to.
pub struct InfoColumns {
    spec: &'static [(&'static str, &'static str, Col)],
    keys: Vec<Option<InfoKey>>,
}

impl InfoColumns {
    pub fn new(spec: &'static [(&'static str, &'static str, Col)], header: &SnvHeader) -> Self {
        let keys = spec.iter().map(|(_, name, _)| header.resolve(name)).collect();
        Self { spec, keys }
    }

    pub fn builders(&self) -> Vec<ScalarBuilder> {
        self.spec.iter().map(|(_, _, c)| ScalarBuilder::new(*c)).collect()
    }

    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.spec.iter().map(|(n, _, _)| *n)
    }

    /// The values of one record, in `spec` order.
    ///
    /// # Safety
    /// `hdr` and `rec` must be valid, `rec` unpacked.
    pub unsafe fn read(&self, hdr: *mut htslib::bcf_hdr_t, rec: *mut htslib::bcf1_t) -> Result<Vec<Scalar>, String> {
        self.spec
            .iter()
            .zip(self.keys.iter())
            .map(|((_, name, col), key)| {
                let v = key.as_ref().and_then(|k| unsafe { k.get(hdr, rec) });
                to_column(v, *col, name)
            })
            .collect()
    }
}

/// What the extraction reads from the VCF header: the INFO lines (`InfoKeys`) and the CSQ columns.
pub struct SnvHeader {
    info_ids: Vec<String>,
    info_flag: HashMap<String, bool>,
    /// Lowercased header ID -> header ID, the last line winning (`InfoKeys`).
    lower: HashMap<String, String>,
    /// Kept as a result: the somatic path reports the sample checks before a missing CSQ line, as Python does.
    csq: Result<CsqIndexes, String>,
}

impl SnvHeader {
    pub fn read(header: &HeaderView) -> Self {
        let mut info_ids: Vec<String> = Vec::new();
        let mut info_flag: HashMap<String, bool> = HashMap::new();
        let mut csq_description: Option<String> = None;
        let mut csq_in_format: Option<String> = None;
        for rec in header.header_records() {
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
        let csq = csq_description
            .or(csq_in_format)
            .map(|d| consequence::parse_csq_header(&d))
            .ok_or_else(|| "KeyError: b'CSQ' (no CSQ header line)".to_string());
        let mut lower = HashMap::new();
        for id in &info_ids {
            lower.insert(id.to_lowercase(), id.clone());
        }
        Self {
            info_ids,
            info_flag,
            lower,
            csq,
        }
    }

    pub fn csq_indexes(&self) -> Result<CsqIndexes, String> {
        self.csq.clone()
    }

    /// The key of an INFO ID as written, whatever the header declares.
    pub fn key_for(&self, id: &str) -> InfoKey {
        InfoKey {
            key: cstring(id),
            is_flag: *self.info_flag.get(id).unwrap_or(&false),
        }
    }

    /// `InfoKeys._key`: the header ID matching `name` ignoring case, then through `INFO_ALIASES`.
    pub fn resolve(&self, name: &str) -> Option<InfoKey> {
        std::iter::once(name)
            .chain(info_aliases(name).iter().copied())
            .find_map(|c| self.lower.get(&c.to_lowercase()))
            .map(|id| self.key_for(id))
    }

    pub fn has_info(&self, id: &str) -> bool {
        self.info_ids.iter().any(|i| i == id)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------------------------------------------

/// `snv/common.py::locus_and_hash`.
pub fn locus_and_hash(chrom: &str, pos: i64, reference: &str, alt: &str) -> (String, String) {
    let locus = format!("{chrom}-{pos}-{reference}-{alt}");
    let hash = hex::encode(Sha256::digest(locus.as_bytes()));
    (locus, hash)
}

/// `Common`.
pub struct Common {
    pub locus: String,
    pub locus_hash: String,
    pub chromosome: String,
    pub start: i32,
    pub end: i32,
    pub reference: String,
    pub alternate: String,
}

/// The genotype views of all the samples of one record (cyvcf2 `genotypes`, `gt_types`, `gt_ref_depths`...).
pub struct Genos {
    pub gts: Vec<i32>,
    pub nper: usize,
    pub ref_depths: Vec<i32>,
    pub alt_depths: Vec<i32>,
}

impl Genos {
    /// `genotypes[i][:-1]`.
    pub fn calls(&self, i: usize) -> Vec<i32> {
        genotype::calls(&self.gts, self.nper, i)
    }

    /// `gt_types[i]`.
    pub fn gt_type(&self, i: usize) -> i32 {
        genotype::gt_type(&self.gts, self.nper, i)
    }

    /// `gt_phases[i]`.
    pub fn phased(&self, i: usize) -> bool {
        genotype::phased(&self.gts, self.nper, i)
    }

    /// `gt_depths[i]`.
    pub fn depth(&self, i: usize) -> i32 {
        genotype::depth(self.ref_depths[i], self.alt_depths[i])
    }

    /// `gt_alt_freqs[i]`.
    pub fn alt_freq(&self, i: usize) -> f64 {
        genotype::alt_freq(self.depth(i), self.alt_depths[i])
    }
}

/// The FORMAT buffers of a reader, reused across records.
pub struct RecordDecoder {
    gt_buf: I32Buf,
    ad_buf: I32Buf,
    ao_buf: I32Buf,
    int_buf: I32Buf,
    float_buf: F32Buf,
    tag_gt: CString,
    tag_ad: CString,
    tag_ro: CString,
    tag_ao: CString,
}

impl RecordDecoder {
    pub fn new() -> Self {
        Self {
            gt_buf: I32Buf::new(),
            ad_buf: I32Buf::new(),
            ao_buf: I32Buf::new(),
            int_buf: I32Buf::new(),
            float_buf: F32Buf::new(),
            tag_gt: cstring("GT"),
            tag_ad: cstring("AD"),
            tag_ro: cstring("RO"),
            tag_ao: cstring("AO"),
        }
    }

    /// The `process.py` record checks then `process_common`: None for a multi-allelic record or a record on an
    /// unsupported chromosome (counted in `stats`).
    pub fn common(header: &HeaderView, rec: &htslib::bcf1_t, stats: &mut Stats) -> Result<Option<Common>, String> {
        let alleles = hts::alleles(rec);
        if alleles.len() > 2 {
            stats.skipped_multi_allelic += 1;
            return Ok(None);
        }
        let chrom_raw = match u32::try_from(rec.rid).ok().and_then(|rid| header.rid2name(rid).ok()) {
            Some(name) => String::from_utf8_lossy(name).into_owned(),
            None => return Err(format!("record with unknown contig id {}", rec.rid)),
        };
        if !SUPPORTED_CHROMOSOMES.contains(&chrom_raw.as_ref()) {
            stats.skipped_unsupported_chromosome += 1;
            return Ok(None);
        }
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
        Ok(Some(Common {
            locus,
            locus_hash,
            start: i32::try_from(pos).map_err(|_| "POS overflows int32")?,
            end: i32::try_from(rec.pos + rec.rlen).map_err(|_| "END overflows int32")?,
            chromosome,
            reference,
            alternate,
        }))
    }

    /// The genotypes and allele depths of the `n` samples.
    ///
    /// # Safety
    /// `hdr` and `rec` must be valid, `rec` unpacked.
    pub unsafe fn genos(
        &mut self,
        hdr: *mut htslib::bcf_hdr_t,
        rec: *mut htslib::bcf1_t,
        n: usize,
        locus: &str,
    ) -> Result<Genos, String> {
        let ngts = unsafe { self.gt_buf.get_format_int32(hdr, rec, &self.tag_gt) };
        if ngts < 0 {
            return Err(format!(
                "error parsing genotypes; they may be absent from this record ({locus})"
            ));
        }
        let nper = ngts as usize / n;
        if nper == 0 {
            return Err(format!("IndexError: record without genotype values ({locus})"));
        }
        let nad = unsafe { self.ad_buf.get_format_int32(hdr, rec, &self.tag_ad) };
        let (ref_depths, alt_depths) = if nad > 0 {
            genotype::ref_alt_depths(DepthSource::Ad(self.ad_buf.values(nad)), n)?
        } else if nad == -1 {
            // Freebayes: RO / AO. `ad_buf` is free to take RO now.
            let nro = unsafe { self.ad_buf.get_format_int32(hdr, rec, &self.tag_ro) };
            let nao = unsafe { self.ao_buf.get_format_int32(hdr, rec, &self.tag_ao) };
            let ro = (nro >= 0).then(|| self.ad_buf.values(nro));
            let ao = (nao >= 0).then(|| self.ao_buf.values(nao));
            genotype::ref_alt_depths(DepthSource::RoAo(ro, ao), n)?
        } else {
            genotype::ref_alt_depths(DepthSource::Unknown, n)?
        };
        Ok(Genos {
            gts: self.gt_buf.values(ngts).to_vec(),
            nper,
            ref_depths,
            alt_depths,
        })
    }

    /// `record.format(tag)[:, 0]` for an Integer field when the record carries the tag, else None.
    ///
    /// # Safety
    /// `hdr` and `rec` must be valid, `rec` unpacked.
    pub unsafe fn first_format_int(
        &mut self,
        hdr: *mut htslib::bcf_hdr_t,
        rec: *mut htslib::bcf1_t,
        tag: &CStr,
        n: usize,
    ) -> Result<Option<Vec<i32>>, String> {
        if !unsafe { hts::has_format(hdr, rec, tag) } {
            return Ok(None);
        }
        let nret = unsafe { self.int_buf.get_format_int32(hdr, rec, tag) };
        if nret < 0 {
            return Err(format!(
                "FORMAT/{} is not an Integer field (htslib {nret}); the Rust engine supports only Integer (the Python engine loads it)",
                tag.to_string_lossy()
            ));
        }
        let nper = nret as usize / n;
        let v = self.int_buf.values(nret);
        Ok(Some((0..n).map(|i| v[i * nper]).collect()))
    }

    /// `record.format(tag)[:, 0]` for a Float field when the record carries the tag, else None. The values are
    /// raw: a missing one is htslib's NaN, as cyvcf2 returns it.
    ///
    /// # Safety
    /// `hdr` and `rec` must be valid, `rec` unpacked.
    pub unsafe fn first_format_float(
        &mut self,
        hdr: *mut htslib::bcf_hdr_t,
        rec: *mut htslib::bcf1_t,
        tag: &CStr,
        n: usize,
    ) -> Result<Option<Vec<f32>>, String> {
        if !unsafe { hts::has_format(hdr, rec, tag) } {
            return Ok(None);
        }
        let nret = unsafe { self.float_buf.get_format_float(hdr, rec, tag) };
        if nret < 0 {
            return Err(format!(
                "FORMAT/{} is not a Float field (htslib {nret}); only Float is supported",
                tag.to_string_lossy()
            ));
        }
        let nper = nret as usize / n;
        let v = self.float_buf.values(nret);
        Ok(Some((0..n).map(|i| v[i * nper]).collect()))
    }
}

/// `int(record.QUAL)` as the float column holds it, and `record.FILTER or "PASS"`.
///
/// # Safety
/// `hdr` and `rec` must be valid, `rec` unpacked.
pub unsafe fn quality_and_filter(hdr: *mut htslib::bcf_hdr_t, rec: &htslib::bcf1_t) -> (Option<f32>, String) {
    let quality = hts::qual(rec).map(|q| q.trunc());
    let filter = unsafe { hts::filter(hdr, rec) }.unwrap_or_else(|| "PASS".to_string());
    (quality, filter)
}

// ---------------------------------------------------------------------------------------------------------------
// Counters
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

impl Stats {
    /// The scalar counters, keyed by name.
    pub fn counts(&self) -> BTreeMap<&'static str, u64> {
        let mut m = BTreeMap::new();
        m.insert("records", self.records);
        m.insert("kept", self.kept);
        m.insert("skipped_no_alt", self.skipped_no_alt);
        m.insert("skipped_multi_allelic", self.skipped_multi_allelic);
        m.insert("skipped_unsupported_chromosome", self.skipped_unsupported_chromosome);
        m.insert("unclassified_sources", self.csq.unclassified);
        m.insert("malformed_comphet", self.malformed_comphet);
        m
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Arrow builders
// ---------------------------------------------------------------------------------------------------------------

pub type Columns = Vec<(&'static str, ArrayRef)>;

/// Iceberg's list element: named `element`, required.
pub fn list_of(item: DataType) -> Field {
    Field::new("element", item, false)
}

pub fn int_list() -> ListBuilder<Int32Builder> {
    ListBuilder::new(Int32Builder::new()).with_field(list_of(DataType::Int32))
}

pub fn str_list() -> ListBuilder<StringBuilder> {
    ListBuilder::new(StringBuilder::new()).with_field(list_of(DataType::Utf8))
}

/// Every column nullable: the Python side, or the Parquet sink, casts the batch to the Iceberg schema.
pub fn batch(cols: Columns) -> Result<RecordBatch, String> {
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

pub struct CommonCols {
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
    pub fn new() -> Self {
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

    pub fn append(&mut self, task_id: i32, c: &Common) {
        self.task_id.append_value(task_id);
        self.locus.append_value(&c.locus);
        self.locus_hash.append_value(&c.locus_hash);
        self.chromosome.append_value(&c.chromosome);
        self.start.append_value(c.start);
        self.end.append_value(c.end);
        self.reference.append_value(&c.reference);
        self.alternate.append_value(&c.alternate);
    }

    pub fn finish(&mut self, cols: &mut Columns) {
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

pub enum ScalarBuilder {
    F32(Float32Builder),
    I32(Int32Builder),
    Bool(BooleanBuilder),
    Str(StringBuilder),
}

impl ScalarBuilder {
    pub fn new(col: Col) -> Self {
        match col {
            Col::F32 => Self::F32(Float32Builder::new()),
            Col::I32 => Self::I32(Int32Builder::new()),
            Col::Bool => Self::Bool(BooleanBuilder::new()),
            Col::Str => Self::Str(StringBuilder::new()),
        }
    }

    pub fn append(&mut self, v: &Scalar) {
        match (self, v) {
            (Self::F32(b), Scalar::F32(v)) => b.append_option(*v),
            (Self::I32(b), Scalar::I32(v)) => b.append_option(*v),
            (Self::Bool(b), Scalar::Bool(v)) => b.append_option(*v),
            (Self::Str(b), Scalar::Str(v)) => b.append_option(v.as_deref()),
            _ => unreachable!("column type fixed at construction"),
        }
    }

    pub fn finish(&mut self) -> ArrayRef {
        match self {
            Self::F32(b) => Arc::new(b.finish()),
            Self::I32(b) => Arc::new(b.finish()),
            Self::Bool(b) => Arc::new(b.finish()),
            Self::Str(b) => Arc::new(b.finish()),
        }
    }
}

/// `snv_variant` (`variant.py::process_variant`).
pub struct VariantCols {
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
    pub fn new() -> Self {
        Self {
            common: CommonCols::new(),
            variant_class: StringBuilder::new(),
            symbol: StringBuilder::new(),
            consequences: str_list(),
            vep_impact: StringBuilder::new(),
            impact_score: Int32Builder::new(),
            mane_select: StringBuilder::new(),
            is_mane_select: BooleanBuilder::new(),
            is_mane_plus: BooleanBuilder::new(),
            is_canonical: BooleanBuilder::new(),
            rsnumber: str_list(),
            hgvsg: StringBuilder::new(),
            hgvsc: StringBuilder::new(),
            hgvsp: StringBuilder::new(),
            dna_change: StringBuilder::new(),
            aa_change: StringBuilder::new(),
            transcript_id: StringBuilder::new(),
            pick_source: StringBuilder::new(),
        }
    }

    pub fn append(&mut self, task_id: i32, c: &Common, id: Option<&str>, picked: Option<&Consequence>) {
        self.common.append(task_id, c);
        // `"rsnumber": record.ID` into a list column: pyarrow reads the Python str as a sequence of characters,
        // so the Python ETL stores ["r", "s", "1", ...]. Kept identical here; see the README.
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

    pub fn finish(&mut self) -> Result<RecordBatch, String> {
        let mut cols: Columns = Vec::new();
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

/// `snv_consequence` (`consequence.py::process_consequence`).
pub struct ConsequenceCols {
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
    pub fn new() -> Self {
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
            consequences: str_list(),
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

    pub fn append(&mut self, task_id: i32, c: &Common, q: &Consequence) {
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

    pub fn finish(&mut self) -> Result<RecordBatch, String> {
        let mut cols: Columns = Vec::new();
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

/// The `snv_variant` and `snv_consequence` rows of a record, written by both extractions.
pub struct VariantTables {
    pub var: VariantCols,
    pub csq: ConsequenceCols,
    csq_key: InfoKey,
}

impl VariantTables {
    pub fn new(header: &SnvHeader) -> Self {
        Self {
            var: VariantCols::new(),
            csq: ConsequenceCols::new(),
            csq_key: header.key_for("CSQ"),
        }
    }

    /// `process_consequence` then `process_variant` for one kept record.
    ///
    /// # Safety
    /// `hdr` and `rec` must be valid, `rec` unpacked.
    pub unsafe fn append(
        &mut self,
        hdr: *mut htslib::bcf_hdr_t,
        rec: *mut htslib::bcf1_t,
        task_id: i32,
        common: &Common,
        csq_idx: &CsqIndexes,
        stats: &mut CsqStats,
    ) -> Result<(), String> {
        let csq_value = unsafe { self.csq_key.get(hdr, rec) };
        let csq_str = match &csq_value {
            None => None,
            Some(InfoValue::Str(s)) => Some(s.as_str()),
            Some(other) => return Err(format!("INFO/CSQ is not a string: {other:?}")),
        };
        let (picked, consequences) = consequence::process_consequence(csq_str, csq_idx, stats)?;
        for q in &consequences {
            self.csq.append(task_id, common, q);
        }
        let id = hts::id(unsafe { &*rec });
        self.var
            .append(task_id, common, id.as_deref(), picked.map(|i| &consequences[i]));
        Ok(())
    }
}
