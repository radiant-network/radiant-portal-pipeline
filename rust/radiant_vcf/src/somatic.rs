//! Somatic SNV extraction: port of `radiant/tasks/vcf/snv/somatic/process.py` and `occurrence.py`. The parts shared
//! with the germline extraction are in `snv.rs`.
//!
//! The sample checks of `process_task` (missing aliquot, tumor-only task on a multi-sample VCF, histology types)
//! stay in Python: the reader exposes the file's samples, Python validates them with the same functions as the
//! Python engine, then hands the tumor and normal indexes to `configure`.

use std::ffi::CString;
use std::sync::Arc;

use arrow::array::{BooleanBuilder, Float32Builder, Int32Builder, ListBuilder, StringBuilder};
use arrow::record_batch::RecordBatch;
use rust_htslib::bcf::{self, Read};
use rust_htslib::htslib;

use crate::consequence::CsqIndexes;
use crate::genotype;
use crate::germline::select_samples;
use crate::hts::{InfoValue, cstring};
use crate::parquet_sink::{self, ParquetSink, WrittenFile};
use crate::snv::{
    self, Col, Columns, CommonCols, Genos, InfoColumns, InfoKey, RecordDecoder, ScalarBuilder, SnvHeader, Stats,
    VariantTables,
};

/// The INFO columns of `somatic_snv_occurrence` (`occurrence.py`), read through `InfoKeys`. `info_cal` is read by
/// the Python code but is not a column of the table, so it is not read here.
const OCC_INFO: &[(&str, &str, Col)] = &[
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
    ("info_mq0", "MQ0", Col::F32),
    ("info_m_qrank_sum", "MQRankSum", Col::F32),
    ("info_qd", "QD", Col::F32),
    ("info_r2_5p_bias", "R2_5P_bias", Col::F32),
    ("info_read_pos_rank_sum", "ReadPosRankSum", Col::F32),
    ("info_sor", "SOR", Col::F32),
    ("info_vqslod", "VQSLOD", Col::F32),
    ("info_culprit", "culprit", Col::Str),
    ("info_dp", "DP", Col::I32),
    ("info_haplotype_score", "HaplotypeScore", Col::F32),
    ("info_hotspotallele", "HotspotAllele", Col::I32),
    ("info_germq", "GERMQ", Col::F32),
    ("info_tlod", "TLOD", Col::F32),
    ("info_mapq", "MAPQ", Col::F32),
    ("info_aq", "AQ", Col::F32),
];

/// The table keys of the somatic extraction, in batch order.
pub const TABLES: [&str; 3] = ["occurrence", "variant", "consequence"];

/// The tumor and normal samples, as `get_sorted_task_experiments` resolves them.
#[derive(Debug, Clone)]
pub struct SomaticConfig {
    /// Index of the tumor sample in the VCF samples kept.
    pub tumor_index: usize,
    /// Index of the normal sample, None for a tumor-only analysis.
    pub normal_index: Option<usize>,
    pub tumor_seq_id: i32,
    pub tenant_code: String,
    pub normal_seq_id: Option<i32>,
}

/// `read_hotspot`: the DRAGEN `hotspot` flag wins over the GATK `HotspotAllele` index (`1` = the alternate).
/// Python truthiness and equality rules.
fn read_hotspot(hotspot: Option<InfoValue>, hotspot_allele: Option<InfoValue>) -> Option<bool> {
    if let Some(v) = hotspot {
        return Some(match v {
            InfoValue::Bool(b) => b,
            InfoValue::Int(i) => i != 0,
            InfoValue::Float(f) => f != 0.0,
            InfoValue::Str(s) => !s.is_empty(),
            InfoValue::List => true,
        });
    }
    hotspot_allele.map(|v| match v {
        InfoValue::Int(i) => i == 1,
        InfoValue::Float(f) => f == 1.0,
        InfoValue::Bool(b) => b,
        InfoValue::Str(_) | InfoValue::List => false,
    })
}

/// One sample's columns (`tumor_*` or `normal_*`).
struct Side {
    calls: Vec<i32>,
    dp: Option<i32>,
    has_alt: bool,
    zygosity: &'static str,
    ad_ref: Option<i32>,
    ad_alt: Option<i32>,
    ad_total: Option<i32>,
    ad_ratio: Option<f32>,
    phased: bool,
    sq: Option<f32>,
}

/// `adjust_somatic_calls_and_zygosity`: an alt call needs at least 2 alt reads, else the calls are unknown.
fn adjust_somatic(calls: Vec<i32>, zygosity: i32, ad_alt: Option<i32>) -> (Vec<i32>, &'static str) {
    if !calls.contains(&1) {
        return (calls, "WT");
    }
    if ad_alt.is_none_or(|a| a < 2) {
        return (vec![-1; calls.len()], "UNK");
    }
    if calls.len() == 1 {
        return (calls, "HEM");
    }
    (calls, genotype::zygosity_name(zygosity))
}

fn side(g: &Genos, i: usize, dp: Option<&[i32]>, sq: Option<&[f32]>) -> Side {
    let r = g.ref_depths[i];
    let a = g.alt_depths[i];
    let ad_alt = (a > 0).then_some(a);
    let (calls, zygosity) = adjust_somatic(g.calls(i), g.gt_type(i), ad_alt);
    let total = g.depth(i);
    let freq = g.alt_freq(i);
    let dp_v = dp.map_or(0, |v| v[i]);
    Side {
        has_alt: calls.contains(&1),
        calls,
        dp: (dp_v > 0).then_some(dp_v),
        zygosity,
        ad_ref: (r > 0).then_some(r),
        ad_alt,
        ad_total: (total > 0).then_some(total),
        ad_ratio: (freq > 0.0).then_some(freq as f32),
        phased: g.phased(i),
        // Raw, as cyvcf2 gives it: a missing value is a NaN, not a null.
        sq: sq.map(|v| v[i]),
    }
}

/// The `tumor_*` or `normal_*` columns.
struct SideCols {
    names: [&'static str; 13],
    seq_id: Int32Builder,
    calls: ListBuilder<Int32Builder>,
    dp: Int32Builder,
    has_alt: BooleanBuilder,
    af: Float32Builder,
    zygosity: StringBuilder,
    ad_ref: Int32Builder,
    ad_alt: Int32Builder,
    ad_total: Int32Builder,
    ad_ratio: Float32Builder,
    phased: BooleanBuilder,
    gt_status: StringBuilder,
    sq: Float32Builder,
}

const TUMOR_COLUMNS: [&str; 13] = [
    "tumor_seq_id",
    "tumor_calls",
    "tumor_dp",
    "tumor_has_alt",
    "tumor_af",
    "tumor_zygosity",
    "tumor_ad_ref",
    "tumor_ad_alt",
    "tumor_ad_total",
    "tumor_ad_ratio",
    "tumor_phased",
    "tumor_gt_status",
    "tumor_sq",
];

const NORMAL_COLUMNS: [&str; 13] = [
    "normal_seq_id",
    "normal_calls",
    "normal_dp",
    "normal_has_alt",
    "normal_af",
    "normal_zygosity",
    "normal_ad_ref",
    "normal_ad_alt",
    "normal_ad_total",
    "normal_ad_ratio",
    "normal_phased",
    "normal_gt_status",
    "normal_sq",
];

impl SideCols {
    fn new(names: [&'static str; 13]) -> Self {
        Self {
            names,
            seq_id: Int32Builder::new(),
            calls: snv::int_list(),
            dp: Int32Builder::new(),
            has_alt: BooleanBuilder::new(),
            af: Float32Builder::new(),
            zygosity: StringBuilder::new(),
            ad_ref: Int32Builder::new(),
            ad_alt: Int32Builder::new(),
            ad_total: Int32Builder::new(),
            ad_ratio: Float32Builder::new(),
            phased: BooleanBuilder::new(),
            gt_status: StringBuilder::new(),
            sq: Float32Builder::new(),
        }
    }

    /// `side` is None for the normal of a tumor-only analysis: every `normal_*` value is null.
    fn append(&mut self, seq_id: Option<i32>, side: Option<&Side>) {
        self.seq_id.append_option(seq_id);
        // `*_gt_status` is always None in the Python code.
        self.gt_status.append_null();
        match side {
            Some(s) => {
                self.calls.values().append_slice(&s.calls);
                self.calls.append(true);
                self.dp.append_option(s.dp);
                self.has_alt.append_value(s.has_alt);
                self.af.append_option(s.ad_ratio);
                self.zygosity.append_value(s.zygosity);
                self.ad_ref.append_option(s.ad_ref);
                self.ad_alt.append_option(s.ad_alt);
                self.ad_total.append_option(s.ad_total);
                self.ad_ratio.append_option(s.ad_ratio);
                self.phased.append_value(s.phased);
                self.sq.append_option(s.sq);
            }
            None => {
                self.calls.append(false);
                self.dp.append_null();
                self.has_alt.append_null();
                self.af.append_null();
                self.zygosity.append_null();
                self.ad_ref.append_null();
                self.ad_alt.append_null();
                self.ad_total.append_null();
                self.ad_ratio.append_null();
                self.phased.append_null();
                self.sq.append_null();
            }
        }
    }

    fn finish(&mut self, cols: &mut Columns) {
        let n = self.names;
        cols.push((n[0], Arc::new(self.seq_id.finish())));
        cols.push((n[1], Arc::new(self.calls.finish())));
        cols.push((n[2], Arc::new(self.dp.finish())));
        cols.push((n[3], Arc::new(self.has_alt.finish())));
        cols.push((n[4], Arc::new(self.af.finish())));
        cols.push((n[5], Arc::new(self.zygosity.finish())));
        cols.push((n[6], Arc::new(self.ad_ref.finish())));
        cols.push((n[7], Arc::new(self.ad_alt.finish())));
        cols.push((n[8], Arc::new(self.ad_total.finish())));
        cols.push((n[9], Arc::new(self.ad_ratio.finish())));
        cols.push((n[10], Arc::new(self.phased.finish())));
        cols.push((n[11], Arc::new(self.gt_status.finish())));
        cols.push((n[12], Arc::new(self.sq.finish())));
    }
}

struct OccurrenceCols {
    common: CommonCols,
    part: Int32Builder,
    tenant_code: StringBuilder,
    quality: Float32Builder,
    filter: StringBuilder,
    info_names: Vec<&'static str>,
    info: Vec<ScalarBuilder>,
    hotspot: BooleanBuilder,
    tumor: SideCols,
    normal: SideCols,
}

impl OccurrenceCols {
    fn new(info: &InfoColumns) -> Self {
        Self {
            common: CommonCols::new(),
            part: Int32Builder::new(),
            tenant_code: StringBuilder::new(),
            quality: Float32Builder::new(),
            filter: StringBuilder::new(),
            info_names: info.names().collect(),
            info: info.builders(),
            hotspot: BooleanBuilder::new(),
            tumor: SideCols::new(TUMOR_COLUMNS),
            normal: SideCols::new(NORMAL_COLUMNS),
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, String> {
        let mut cols: Columns = Vec::new();
        self.common.finish(&mut cols);
        cols.push(("part", Arc::new(self.part.finish())));
        cols.push(("tenant_code", Arc::new(self.tenant_code.finish())));
        cols.push(("quality", Arc::new(self.quality.finish())));
        cols.push(("filter", Arc::new(self.filter.finish())));
        for (b, name) in self.info.iter_mut().zip(self.info_names.iter()) {
            cols.push((name, b.finish()));
        }
        cols.push(("info_hotspot", Arc::new(self.hotspot.finish())));
        self.tumor.finish(&mut cols);
        self.normal.finish(&mut cols);
        snv::batch(cols)
    }
}

pub struct SomaticReader {
    reader: bcf::Reader,
    record: bcf::Record,
    hdr: *mut htslib::bcf_hdr_t,
    task_id: i32,
    part: i32,
    /// The samples of the file, before the task's aliquots are selected.
    pub file_samples: Vec<String>,
    /// The samples kept, in file order.
    pub samples: Vec<String>,
    header: SnvHeader,
    cfg: Option<(SomaticConfig, CsqIndexes)>,
    occ_info: InfoColumns,
    hotspot_key: InfoKey,
    hotspot_allele_key: InfoKey,
    dec: RecordDecoder,
    tag_dp: CString,
    tag_sq: CString,
    occ: OccurrenceCols,
    tables: VariantTables,
    pub stats: Stats,
    done: bool,
}

// The raw pointers are owned by `reader` / the buffers and only touched by the one thread holding `&mut self`.
unsafe impl Send for SomaticReader {}

impl SomaticReader {
    /// Opens the VCF and selects the task's aliquots. Never fails on the samples: Python checks them.
    pub fn open(
        path: &str,
        aliquots: &[String],
        task_id: i32,
        part: i32,
        threads: Option<usize>,
    ) -> Result<Self, String> {
        let mut reader = bcf::Reader::from_path(path).map_err(|e| format!("cannot open {path}: {e}"))?;
        if let Some(n) = threads.filter(|n| *n > 0) {
            reader.set_threads(n).map_err(|e| e.to_string())?;
        }
        let hdr = unsafe { reader.header().as_ptr() };
        let file_samples: Vec<String> = reader
            .header()
            .samples()
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect();
        let aliquots: Vec<&str> = aliquots.iter().map(String::as_str).collect();
        let samples = select_samples(&reader, &aliquots, path)?;
        let header = SnvHeader::read(reader.header());
        let occ_info = InfoColumns::new(OCC_INFO, &header);
        let record = reader.empty_record();
        Ok(Self {
            reader,
            record,
            hdr,
            task_id,
            part,
            file_samples,
            samples,
            occ: OccurrenceCols::new(&occ_info),
            occ_info,
            // `read_hotspot` reads both keys as spelled, not through `InfoKeys`.
            hotspot_key: header.key_for("hotspot"),
            hotspot_allele_key: header.key_for("HotspotAllele"),
            tables: VariantTables::new(&header),
            header,
            cfg: None,
            dec: RecordDecoder::new(),
            tag_dp: cstring("DP"),
            tag_sq: cstring("SQ"),
            stats: Stats::default(),
            done: false,
        })
    }

    /// Sets the tumor and normal samples, once Python has checked them. Fails here, as Python's
    /// `parse_csq_header` does after the checks, when the header has no CSQ line.
    pub fn configure(&mut self, cfg: SomaticConfig) -> Result<(), String> {
        let n = self.samples.len();
        if cfg.tumor_index >= n || cfg.normal_index.is_some_and(|i| i >= n) {
            return Err(format!("sample index out of range for {n} samples: {cfg:?}"));
        }
        if cfg.normal_index.is_some() != cfg.normal_seq_id.is_some() {
            return Err(format!(
                "normal_index and normal_seq_id must be both set or both None: {cfg:?}"
            ));
        }
        self.cfg = Some((cfg, self.header.csq_indexes()?));
        Ok(())
    }

    /// Reads the whole file into one Parquet sink per table of `TABLES`. Returns the files of each table.
    pub fn write_parquet(
        &mut self,
        sinks: &mut [(String, ParquetSink)],
        batch_records: usize,
    ) -> Result<Vec<(String, Vec<WrittenFile>)>, String> {
        parquet_sink::drain(sinks, &TABLES, || self.next_batches(batch_records))
    }

    /// Reads until `max_records` records are kept or the file ends, then returns one batch per table of `TABLES`.
    /// None once the file is exhausted.
    pub fn next_batches(&mut self, max_records: usize) -> Result<Option<Vec<RecordBatch>>, String> {
        if self.cfg.is_none() {
            return Err("configure() must be called before reading".into());
        }
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
        Ok(Some(vec![
            self.occ.finish()?,
            self.tables.var.finish()?,
            self.tables.csq.finish()?,
        ]))
    }

    /// One record. True when it reached the tables.
    fn process_record(&mut self) -> Result<bool, String> {
        let hdr = self.hdr;
        let rec_ptr: *mut htslib::bcf1_t = self.record.inner_mut();
        let rec: &htslib::bcf1_t = unsafe { &*rec_ptr };
        let Some(common) = RecordDecoder::common(self.reader.header(), rec, &mut self.stats)? else {
            return Ok(false);
        };
        let (cfg, csq_idx) = self.cfg.as_ref().expect("checked by next_batches");

        // --- process_occurrence
        let n = self.samples.len();
        let g = unsafe { self.dec.genos(hdr, rec_ptr, n, &common.locus) }?;
        let dp = unsafe { self.dec.first_format_int(hdr, rec_ptr, &self.tag_dp, n) }?;
        let sq = unsafe { self.dec.first_format_float(hdr, rec_ptr, &self.tag_sq, n) }?;
        let tumor = side(&g, cfg.tumor_index, dp.as_deref(), sq.as_deref());
        // RAD-75: a record the tumor does not carry is dropped from all three tables together.
        if !tumor.has_alt {
            self.stats.skipped_no_alt += 1;
            return Ok(false);
        }
        let normal = cfg.normal_index.map(|i| side(&g, i, dp.as_deref(), sq.as_deref()));

        let info = unsafe { self.occ_info.read(hdr, rec_ptr) }?;
        let hotspot = read_hotspot(unsafe { self.hotspot_key.get(hdr, rec_ptr) }, unsafe {
            self.hotspot_allele_key.get(hdr, rec_ptr)
        });
        let (quality, filter) = unsafe { snv::quality_and_filter(hdr, rec) };

        let o = &mut self.occ;
        o.common.append(self.task_id, &common);
        o.part.append_value(self.part);
        o.tenant_code.append_value(&cfg.tenant_code);
        o.quality.append_option(quality);
        o.filter.append_value(&filter);
        for (b, v) in o.info.iter_mut().zip(info.iter()) {
            b.append(v);
        }
        o.hotspot.append_option(hotspot);
        o.tumor.append(Some(cfg.tumor_seq_id), Some(&tumor));
        o.normal.append(cfg.normal_seq_id, normal.as_ref());

        // --- process_consequence + process_variant
        unsafe {
            self.tables
                .append(hdr, rec_ptr, self.task_id, &common, csq_idx, &mut self.stats.csq)
        }?;

        self.stats.kept += 1;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn somatic_zygosity() {
        assert_eq!(
            adjust_somatic(vec![0, 0], genotype::HOM_REF, Some(10)),
            (vec![0, 0], "WT")
        );
        assert_eq!(adjust_somatic(vec![], genotype::UNKNOWN, None), (vec![], "WT"));
        assert_eq!(
            adjust_somatic(vec![0, 1], genotype::HET, Some(1)),
            (vec![-1, -1], "UNK")
        );
        assert_eq!(adjust_somatic(vec![0, 1], genotype::HET, None), (vec![-1, -1], "UNK"));
        assert_eq!(adjust_somatic(vec![1], genotype::HOM_ALT, Some(2)), (vec![1], "HEM"));
        assert_eq!(adjust_somatic(vec![0, 1], genotype::HET, Some(2)), (vec![0, 1], "HET"));
    }

    #[test]
    fn hotspot_rules() {
        assert_eq!(
            read_hotspot(Some(InfoValue::Bool(true)), Some(InfoValue::Int(0))),
            Some(true)
        );
        assert_eq!(read_hotspot(None, Some(InfoValue::Int(1))), Some(true));
        assert_eq!(read_hotspot(None, Some(InfoValue::Int(2))), Some(false));
        assert_eq!(read_hotspot(None, Some(InfoValue::Float(1.0))), Some(true));
        assert_eq!(read_hotspot(None, Some(InfoValue::Str("1".into()))), Some(false));
        assert_eq!(read_hotspot(None, None), None);
    }
}
