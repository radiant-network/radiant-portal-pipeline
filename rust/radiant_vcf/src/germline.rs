//! Germline SNV extraction: port of `radiant/tasks/vcf/snv/germline/process.py` and the modules it calls
//! (`occurrence.py`, `comphet.py`, `inheritance.py`); the parts shared with the somatic extraction are in `snv.rs`.
//!
//! Records are decoded once and appended to Arrow builders, one per output table.

use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::sync::Arc;

use arrow::array::{BooleanBuilder, Float32Builder, Int32Builder, ListBuilder, StringBuilder};
use arrow::record_batch::RecordBatch;
use rust_htslib::bcf::{self, Read};
use rust_htslib::htslib;

use crate::consequence::CsqIndexes;
use crate::genotype;
use crate::hts::{InfoValue, cstring};
use crate::inheritance::{self, SlivarTags};
use crate::parquet_sink::{self, ParquetSink, WrittenFile};
use crate::snv::{
    self, Col, Columns, Common, CommonCols, InfoColumns, InfoKey, RecordDecoder, ScalarBuilder, SnvHeader, Stats,
    VariantTables,
};

const PROGENY_ROLES: [&str; 4] = ["proband", "brother", "sister", "sibling"];

/// The INFO columns of `germline_snv_occurrence` (`occurrence.py`).
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

/// `Experiment`, reduced to the fields the germline extraction reads.
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

/// The parent columns (`father_*` / `mother_*`) of the occurrence table.
struct ParentCols {
    names: [&'static str; 8],
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
    fn new(names: [&'static str; 8]) -> Self {
        Self {
            names,
            dp: Int32Builder::new(),
            gq: Int32Builder::new(),
            ad_ref: Int32Builder::new(),
            ad_alt: Int32Builder::new(),
            ad_total: Int32Builder::new(),
            ad_ratio: Float32Builder::new(),
            calls: snv::int_list(),
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

    fn finish(&mut self, cols: &mut Columns) {
        let n = self.names;
        cols.push((n[0], Arc::new(self.dp.finish())));
        cols.push((n[1], Arc::new(self.gq.finish())));
        cols.push((n[2], Arc::new(self.ad_ref.finish())));
        cols.push((n[3], Arc::new(self.ad_alt.finish())));
        cols.push((n[4], Arc::new(self.ad_total.finish())));
        cols.push((n[5], Arc::new(self.ad_ratio.finish())));
        cols.push((n[6], Arc::new(self.calls.finish())));
        cols.push((n[7], Arc::new(self.zygosity.finish())));
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
    info_names: Vec<&'static str>,
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
}

impl OccurrenceCols {
    fn new(info: &InfoColumns) -> Self {
        Self {
            common: CommonCols::new(),
            part: Int32Builder::new(),
            seq_id: Int32Builder::new(),
            tenant_code: StringBuilder::new(),
            aliquot: StringBuilder::new(),
            dp: Int32Builder::new(),
            gq: Int32Builder::new(),
            calls: snv::int_list(),
            has_alt: BooleanBuilder::new(),
            quality: Float32Builder::new(),
            filter: StringBuilder::new(),
            info_names: info.names().collect(),
            info: info.builders(),
            zygosity: StringBuilder::new(),
            ad_ref: Int32Builder::new(),
            ad_alt: Int32Builder::new(),
            ad_total: Int32Builder::new(),
            ad_ratio: Float32Builder::new(),
            phased: BooleanBuilder::new(),
            parental_origin: StringBuilder::new(),
            father: ParentCols::new([
                "father_dp",
                "father_gq",
                "father_ad_ref",
                "father_ad_alt",
                "father_ad_total",
                "father_ad_ratio",
                "father_calls",
                "father_zygosity",
            ]),
            mother: ParentCols::new([
                "mother_dp",
                "mother_gq",
                "mother_ad_ref",
                "mother_ad_alt",
                "mother_ad_total",
                "mother_ad_ratio",
                "mother_calls",
                "mother_zygosity",
            ]),
            transmission_mode: StringBuilder::new(),
            other_transmission_modes: snv::str_list(),
            transmission_mode_candidate: BooleanBuilder::new(),
            parental_origin_source: StringBuilder::new(),
            transmission_mode_source: StringBuilder::new(),
        }
    }

    fn finish(&mut self) -> Result<RecordBatch, String> {
        let mut cols: Columns = Vec::new();
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
        for (b, name) in self.info.iter_mut().zip(self.info_names.iter()) {
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
        snv::batch(cols)
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
        snv::batch(vec![
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

/// The table keys of the germline extraction, in batch order.
pub const TABLES: [&str; 4] = ["occurrence", "variant", "consequence", "comphet"];

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
    occ_info: InfoColumns,
    comphet_key: InfoKey,
    slivar_keys: Vec<(&'static str, InfoKey)>,
    dec: RecordDecoder,
    tag_dp: CString,
    tag_gq: CString,
    occ: OccurrenceCols,
    tables: VariantTables,
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
        let aliquots: Vec<&str> = experiments.iter().map(|e| e.aliquot.as_str()).collect();
        let samples = select_samples(&reader, &aliquots, path)?;
        if samples.is_empty() {
            return Err(format!("Task {task_id} has no matching samples in the VCF file {path}"));
        }

        let header = SnvHeader::read(reader.header());
        let csq_idx = header.csq_indexes()?;
        let occ_info = InfoColumns::new(OCC_INFO, &header);
        let slivar_parental_origin = inheritance::PO_TAGS.iter().any(|(t, _)| header.has_info(t));
        let slivar_transmission_mode = header.has_info(inheritance::MODE_TAGS_MARKER);
        let slivar_keys = inheritance::PO_TAGS
            .iter()
            .map(|(t, _)| *t)
            .chain(inheritance::MODE_TAGS.iter().map(|(t, _)| *t))
            .chain(inheritance::GENOTYPE_TAGS.iter().copied())
            .chain(std::iter::once(inheritance::CANDIDATE_TAG))
            .map(|t| (t, header.key_for(t)))
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
            occ: OccurrenceCols::new(&occ_info),
            occ_info,
            comphet_key: header.key_for("slivar_comphet"),
            slivar_keys,
            dec: RecordDecoder::new(),
            tag_dp: cstring("DP"),
            tag_gq: cstring("GQ"),
            tables: VariantTables::new(&header),
            comphet: ComphetCols::new(),
            stats: Stats::default(),
            done: false,
        })
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
            self.comphet.finish()?,
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

        // --- process_occurrence
        let n = self.samples.len();
        let g = unsafe { self.dec.genos(hdr, rec_ptr, n, &common.locus) }?;
        let dp = unsafe { self.dec.first_format_int(hdr, rec_ptr, &self.tag_dp, n) }?;
        let gq = unsafe { self.dec.first_format_int(hdr, rec_ptr, &self.tag_gq, n) }?;

        let mut occs: Vec<Occ> = Vec::with_capacity(self.ped.experiments.len());
        for idx in 0..self.ped.experiments.len() {
            let r = g.ref_depths[idx];
            let a = g.alt_depths[idx];
            let ad_ref = (r > 0).then_some(r);
            let ad_alt = (a > 0).then_some(a);
            let zyg = g.gt_type(idx);
            let mut calls = g.calls(idx);
            // `adjust_calls_and_zygosity`.
            let zygosity: &'static str = if (ad_alt.is_some_and(|a| a < 3)
                && (zyg == genotype::HET || zyg == genotype::HOM_ALT))
                || (ad_ref.is_some_and(|r| r < 3) && zyg == genotype::HOM_REF)
            {
                calls = vec![-1; calls.len()];
                "UNK"
            } else if zyg == genotype::HOM_ALT && calls.len() == 1 {
                "HEM"
            } else {
                genotype::zygosity_name(zyg)
            };
            let total = g.depth(idx);
            let freq = g.alt_freq(idx);
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
                phased: g.phased(idx),
            });
        }

        // RAD-75: a record no sample carries is dropped from all the tables together.
        if !occs.iter().any(|o| o.has_alt) {
            self.stats.skipped_no_alt += 1;
            return Ok(false);
        }

        let family = self.family_columns(rec_ptr, &common, &occs);
        let info = unsafe { self.occ_info.read(hdr, rec_ptr) }?;
        let (quality, filter) = unsafe { snv::quality_and_filter(hdr, rec) };
        let position = |s: Option<i32>| s.and_then(|s| self.ped.experiments.iter().position(|e| e.seq_id == s));
        let father_idx = position(self.ped.father_seq_id);
        let mother_idx = position(self.ped.mother_seq_id);

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
        }

        // --- process_consequence + process_variant
        unsafe {
            self.tables
                .append(hdr, rec_ptr, self.task_id, &common, &self.csq_idx, &mut self.stats.csq)
        }?;

        // --- process_comphet
        self.comphet_rows(rec_ptr, &common)?;

        self.stats.kept += 1;
        Ok(true)
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
            if let Some(v) = unsafe { key.get(self.hdr, rec) }
                && let Some(s) = v.as_py_str()
            {
                listed.insert(*tag, s.split(',').map(str::to_string).collect());
            }
        }
        SlivarTags { listed }
    }

    fn comphet_rows(&mut self, rec: *mut htslib::bcf1_t, common: &Common) -> Result<(), String> {
        let value = match unsafe { self.comphet_key.get(self.hdr, rec) } {
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
            let (_, partner) = snv::locus_and_hash(&chrom.replace("chr", ""), pos, r, a);
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

/// `bcf_hdr_set_samples` with the comma-joined aliquots, then the samples kept, in file order.
pub fn select_samples(reader: &bcf::Reader, aliquots: &[&str], path: &str) -> Result<Vec<String>, String> {
    let hdr = unsafe { reader.header().as_ptr() };
    let wanted = cstring(&aliquots.join(","));
    let ret = unsafe { htslib::bcf_hdr_set_samples(hdr, wanted.as_ptr(), 0) };
    if ret < 0 {
        return Err(format!(
            "cannot select samples in {path} (bcf_hdr_set_samples returned {ret})"
        ));
    }
    Ok(reader
        .header()
        .samples()
        .iter()
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect())
}

/// `str.isdigit()` for the ASCII digits a VCF position holds.
fn is_py_digit(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit())
}
