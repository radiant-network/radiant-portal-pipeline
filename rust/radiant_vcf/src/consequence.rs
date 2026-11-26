//! Port of `radiant/tasks/vcf/snv/consequence.py`: the VEP `CSQ` blocks of a record.

use std::collections::{BTreeMap, HashMap};

pub const MANE_SELECT_FLAG: &str = "MANE_Select";
pub const MANE_PLUS_CLINICAL_FLAG: &str = "MANE_Plus_Clinical";

/// `CsqIndexes`: the column of each CSQ field the extraction reads, None when the header lacks it.
#[derive(Debug, Clone, Default)]
pub struct CsqIndexes {
    pub exon: Option<usize>,
    pub impact: Option<usize>,
    pub hgvsg: Option<usize>,
    pub hgvsp: Option<usize>,
    pub hgvsc: Option<usize>,
    pub pick: Option<usize>,
    pub feature: Option<usize>,
    pub gene: Option<usize>,
    pub mane: Option<usize>,
    pub mane_select: Option<usize>,
    pub variant_class: Option<usize>,
    pub symbol: Option<usize>,
    pub source: Option<usize>,
    pub biotype: Option<usize>,
    pub strand: Option<usize>,
    pub consequence: Option<usize>,
    pub canonical: Option<usize>,
    pub cds_position: Option<usize>,
}

/// `parse_csq_header`: the column indexes, from the `Description` of the CSQ header line.
pub fn parse_csq_header(description: &str) -> CsqIndexes {
    let meta = description.rsplit("Format:").next().unwrap_or("");
    let meta = meta.trim_matches(|c| c == ' ' || c == '"');
    // Python dict comprehension: a repeated (lowercased) name keeps its last index.
    let mut map: HashMap<String, usize> = HashMap::new();
    if !meta.is_empty() {
        for (i, f) in meta.split('|').enumerate() {
            map.insert(f.to_lowercase(), i);
        }
    }
    let g = |k: &str| map.get(k).copied();
    CsqIndexes {
        exon: g("exon"),
        impact: g("impact"),
        hgvsg: g("hgvsg"),
        hgvsp: g("hgvsp"),
        hgvsc: g("hgvsc"),
        pick: g("pick"),
        feature: g("feature"),
        gene: g("gene"),
        mane: g("mane"),
        mane_select: g("mane_select"),
        variant_class: g("variant_class"),
        symbol: g("symbol"),
        source: g("source"),
        biotype: g("biotype"),
        strand: g("strand"),
        consequence: g("consequence"),
        canonical: g("canonical"),
        cds_position: g("cds_position"),
    }
}

/// One consequence row. Strings keep the Python distinction: `None` for an absent column, `""` for an empty one.
#[derive(Debug, Clone)]
pub struct Consequence<'a> {
    pub variant_class: Option<&'a str>,
    pub hgvsg: Option<&'a str>,
    pub hgvsp: Option<&'a str>,
    pub hgvsc: Option<&'a str>,
    pub symbol: Option<&'a str>,
    pub transcript_id: Option<&'a str>,
    pub transcript_version: Option<&'a str>,
    pub source: Option<&'static str>,
    pub biotype: Option<&'a str>,
    pub strand: Option<&'a str>,
    pub exon: Option<(&'a str, &'a str)>,
    pub vep_impact: Option<&'a str>,
    pub consequences: Vec<&'a str>,
    pub mane_select: Option<&'a str>,
    pub mane_pair_transcript_id: Option<&'a str>,
    pub is_mane_select: bool,
    pub is_mane_plus: bool,
    pub is_picked: bool,
    pub is_canonical: bool,
    pub aa_change: Option<&'a str>,
    pub dna_change: Option<&'a str>,
    pub impact_score: i32,
    pub cds_start: Option<i32>,
    pub cds_end: Option<i32>,
}

/// Per-file counters `log_source_counts` and `log_invalid_cds` report, plus the unknown SOURCE values to warn on once.
#[derive(Debug, Default)]
pub struct CsqStats {
    pub source_counts: BTreeMap<&'static str, u64>,
    pub unclassified: u64,
    pub invalid_cds: BTreeMap<String, u64>,
    pub unknown_sources: Vec<String>,
}

/// `strip_transcript_version`: up to the first `.`; None and `""` unchanged.
pub fn strip_transcript_version(v: Option<&str>) -> Option<&str> {
    v.map(|s| s.split('.').next().unwrap_or(""))
}

/// `extract_transcript_version`.
pub fn extract_transcript_version(v: Option<&str>) -> Option<&str> {
    let s = v?;
    s.split_once('.').map(|(_, rest)| rest)
}

fn ensembl_feature(t: &str) -> bool {
    t.starts_with("ENST") || t.starts_with("ENSR") || t.starts_with("LRG_")
}

fn refseq_feature(t: &str) -> bool {
    let b = t.as_bytes();
    b.len() >= 3 && matches!(b[0], b'N' | b'X') && matches!(b[1], b'M' | b'R' | b'P') && b[2] == b'_'
}

fn ensembl_gene(g: &str) -> bool {
    g.len() > 4 && g.starts_with("ENSG") && g[4..].bytes().all(|c| c.is_ascii_digit())
}

fn refseq_gene(g: &str) -> bool {
    !g.is_empty() && g.bytes().all(|c| c.is_ascii_digit())
}

/// `resolve_source`.
pub fn resolve_source(
    source: Option<&str>,
    transcript_id: Option<&str>,
    gene_id: Option<&str>,
    stats: &mut CsqStats,
) -> Option<&'static str> {
    let source = source.unwrap_or("").trim();
    let transcript_id = transcript_id.unwrap_or("").trim();
    let gene_id = gene_id.unwrap_or("").trim();
    if !source.is_empty() {
        match source.to_lowercase().as_str() {
            "ensembl" => return Some("Ensembl"),
            "refseq" => return Some("RefSeq"),
            _ => {
                if !stats.unknown_sources.iter().any(|s| s == source) {
                    stats.unknown_sources.push(source.to_string());
                }
            }
        }
    }
    if !transcript_id.is_empty() {
        if ensembl_feature(transcript_id) {
            return Some("Ensembl");
        }
        if refseq_feature(transcript_id) {
            return Some("RefSeq");
        }
    }
    if !gene_id.is_empty() {
        if ensembl_gene(gene_id) {
            return Some("Ensembl");
        }
        if refseq_gene(gene_id) {
            return Some("RefSeq");
        }
    }
    None
}

fn parse_digits(s: &str) -> Option<i32> {
    if !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()) {
        s.parse().ok()
    } else {
        None
    }
}

/// `parse_cds_position`, for a non-empty value. `Err` for an unknown shape.
pub fn parse_cds_position(value: &str) -> Result<(Option<i32>, Option<i32>), ()> {
    if let Some(n) = parse_digits(value) {
        return Ok((Some(n), Some(n)));
    }
    let (start, end) = value.split_once('-').ok_or(())?;
    let side = |s: &str| -> Result<Option<i32>, ()> {
        if s == "?" {
            Ok(None)
        } else {
            parse_digits(s).map(Some).ok_or(())
        }
    };
    let (start, end) = (side(start)?, side(end)?);
    if start.is_none() && end.is_none() {
        return Err(());
    }
    Ok((start, end))
}

fn impact_score(impact: Option<&str>) -> i32 {
    match impact {
        Some("HIGH") => 4,
        Some("MODERATE") => 3,
        Some("LOW") => 2,
        Some("MODIFIER") => 1,
        _ => 0,
    }
}

/// `process_consequence`: every block of the CSQ value, and the index of the picked one.
///
/// The picked block is the last one with `PICK=1`, else the first with `CANONICAL=YES`.
pub fn process_consequence<'a>(
    csq: Option<&'a str>,
    idx: &CsqIndexes,
    stats: &mut CsqStats,
) -> Result<(Option<usize>, Vec<Consequence<'a>>), String> {
    let mut out = Vec::new();
    let mut fields: Vec<&'a str> = Vec::new();
    let mut picked_idx = None;
    let Some(csq) = csq.filter(|s| !s.is_empty()) else {
        return Ok((None, out));
    };
    for block in csq.split(',') {
        fields.clear();
        fields.extend(block.split('|'));
        let get =
            |i: Option<usize>| -> Result<Option<&'a str>, String> {
                match i {
                    None => Ok(None),
                    Some(i) => fields.get(i).copied().map(Some).ok_or_else(|| {
                        format!("CSQ block has {} fields, column {} expected: {block}", fields.len(), i)
                    }),
                }
            };
        let exon_field = get(idx.exon)?;
        // Python `exon_field.split("/")`, kept only when it has exactly two parts.
        let exon = match exon_field {
            Some(e) if !e.is_empty() => match e.split_once('/') {
                Some((rank, total)) if !total.contains('/') => Some((rank, total)),
                _ => None,
            },
            _ => None,
        };
        let vep_impact = get(idx.impact)?;
        let hgvsg = get(idx.hgvsg)?;
        let hgvsp = get(idx.hgvsp)?;
        let hgvsc = get(idx.hgvsc)?;
        let picked = get(idx.pick)? == Some("1");
        let feature = get(idx.feature)?;
        let transcript_id = strip_transcript_version(feature);
        let gene_id = get(idx.gene)?;
        let mane = get(idx.mane)?;
        let mane_select = get(idx.mane_select)?;
        let cds_position = get(idx.cds_position)?;
        let (mut cds_start, mut cds_end) = (None, None);
        if let Some(c) = cds_position.filter(|c| !c.is_empty()) {
            match parse_cds_position(c) {
                Ok((s, e)) => {
                    cds_start = s;
                    cds_end = e;
                }
                Err(()) => *stats.invalid_cds.entry(c.to_string()).or_default() += 1,
            }
        }
        let consequences = get(idx.consequence)?
            .ok_or("CSQ header has no Consequence column ('NoneType' object has no attribute 'split')")?
            .split('&')
            .collect();
        let source = resolve_source(get(idx.source)?, transcript_id, gene_id, stats);
        match source {
            Some(s) => *stats.source_counts.entry(s).or_default() += 1,
            None => stats.unclassified += 1,
        }
        let after_colon = |v: Option<&'a str>| v.filter(|s| !s.is_empty()).map(|s| s.rsplit(':').next().unwrap_or(""));
        let c = Consequence {
            variant_class: get(idx.variant_class)?,
            hgvsg,
            hgvsp,
            hgvsc,
            symbol: get(idx.symbol)?,
            transcript_version: extract_transcript_version(feature),
            transcript_id,
            source,
            biotype: get(idx.biotype)?,
            strand: get(idx.strand)?,
            exon,
            vep_impact,
            consequences,
            mane_select,
            mane_pair_transcript_id: strip_transcript_version(mane_select),
            is_mane_select: mane == Some(MANE_SELECT_FLAG),
            is_mane_plus: mane == Some(MANE_PLUS_CLINICAL_FLAG),
            is_picked: picked,
            is_canonical: get(idx.canonical)? == Some("YES"),
            aa_change: after_colon(hgvsp),
            dna_change: after_colon(hgvsc),
            impact_score: impact_score(vep_impact),
            cds_start,
            cds_end,
        };
        if picked {
            picked_idx = Some(out.len());
        }
        out.push(c);
    }
    if picked_idx.is_none() {
        picked_idx = out.iter().position(|c| c.is_canonical);
    }
    Ok((picked_idx, out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cds_shapes() {
        assert_eq!(parse_cds_position("123"), Ok((Some(123), Some(123))));
        assert_eq!(parse_cds_position("123-125"), Ok((Some(123), Some(125))));
        assert_eq!(parse_cds_position("?-125"), Ok((None, Some(125))));
        assert_eq!(parse_cds_position("123-?"), Ok((Some(123), None)));
        assert_eq!(parse_cds_position("?-?"), Err(()));
        assert_eq!(parse_cds_position("12a"), Err(()));
    }

    #[test]
    fn sources() {
        let mut s = CsqStats::default();
        assert_eq!(resolve_source(Some("refseq"), None, None, &mut s), Some("RefSeq"));
        assert_eq!(resolve_source(Some("x"), Some("ENST1"), None, &mut s), Some("Ensembl"));
        assert_eq!(resolve_source(None, Some("NM_1"), None, &mut s), Some("RefSeq"));
        assert_eq!(resolve_source(None, Some(""), Some("ENSG001"), &mut s), Some("Ensembl"));
        assert_eq!(resolve_source(None, None, Some("7157"), &mut s), Some("RefSeq"));
        assert_eq!(resolve_source(None, None, Some("ENSG"), &mut s), None);
        assert_eq!(s.unknown_sources, vec!["x".to_string()]);
    }

    #[test]
    fn versions() {
        assert_eq!(strip_transcript_version(Some("NM_1.6")), Some("NM_1"));
        assert_eq!(strip_transcript_version(Some("")), Some(""));
        assert_eq!(extract_transcript_version(Some("NM_1.6.2")), Some("6.2"));
        assert_eq!(extract_transcript_version(Some("ENST1")), None);
    }
}
