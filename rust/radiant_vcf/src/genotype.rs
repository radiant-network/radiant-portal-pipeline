//! The per-sample genotype views of cyvcf2 v0.31.1, computed from htslib's raw FORMAT buffers.
//!
//! Each function is a line-for-line port of a cyvcf2 property (`cyvcf2/cyvcf2.pyx`) or helper
//! (`cyvcf2/helpers.c::as_gts`), including its quirks, because the stored values depend on them.

use crate::hts::INT32_VECTOR_END;

/// `ZYGOSITY_*` of `radiant/tasks/vcf/vcf_utils.py` (cyvcf2 `gt_types` with `gts012=False`).
pub const HOM_REF: i32 = 0;
pub const HET: i32 = 1;
pub const UNKNOWN: i32 = 2;
pub const HOM_ALT: i32 = 3;

/// `ZYGOSITY[z]` of `vcf_utils.py`.
pub fn zygosity_name(z: i32) -> &'static str {
    match z {
        HOM_REF => "WT",
        HET => "HET",
        HOM_ALT => "HOM",
        _ => "UNK",
    }
}

#[inline]
fn gt_is_missing(v: i32) -> bool {
    // `bcf_gt_is_missing(val) ((val)>>1 ? 0 : 1)`
    (v >> 1) == 0
}

#[inline]
fn gt_allele(v: i32) -> i32 {
    // `bcf_gt_allele(val) (((val)>>1)-1)`
    (v >> 1) - 1
}

/// `Variant.genotypes[i][:-1]`: the alleles of one sample, -1 for a missing one, stopping at the padding.
pub fn calls(gts: &[i32], nper: usize, sample: usize) -> Vec<i32> {
    let mut out = Vec::with_capacity(nper);
    for &v in &gts[sample * nper..sample * nper + nper] {
        if gt_is_missing(v) {
            out.push(-1);
            continue;
        }
        if v == INT32_VECTOR_END {
            break;
        }
        out.push(gt_allele(v));
    }
    out
}

/// `Variant.gt_types[i]` with `strict_gt=True` (`as_gts(..., strict_gt=1, HOM_ALT=3, UNKNOWN=2)`).
pub fn gt_type(gts: &[i32], nper: usize, sample: usize) -> i32 {
    let i = sample * nper;
    let missing = gts[i..i + nper].iter().filter(|&&v| gt_is_missing(v)).count();
    if missing == nper || missing != 0 {
        return UNKNOWN;
    }
    if nper == 1 || gts[i + 1] == INT32_VECTOR_END {
        return match gt_allele(gts[i]) {
            0 => HOM_REF,
            1 => HOM_ALT,
            _ => UNKNOWN,
        };
    }
    let (a, b) = (gt_allele(gts[i]), gt_allele(gts[i + 1]));
    if a == 0 && b == 0 {
        HOM_REF
    } else if (a == 1 && b == 1) || a == b {
        HOM_ALT
    } else {
        HET
    }
}

/// `Variant.gt_phases[i]`: `raw[i*nper] > 0 and bcf_gt_is_phased(raw[i*nper + 1])`.
///
/// cyvcf2 reads `raw[i*nper + 1]` whatever the ploidy, so a haploid sample takes the phase bit of the next
/// sample's allele; for the last haploid sample it reads past the array (undefined), taken here as unphased.
pub fn phased(gts: &[i32], nper: usize, sample: usize) -> bool {
    let i = sample * nper;
    gts[i] > 0 && gts.get(i + 1).is_some_and(|&v| v & 1 == 1)
}

/// Where `gt_ref_depths` / `gt_alt_depths` read from.
pub enum DepthSource<'a> {
    /// GATK `AD`, `nret > 0`.
    Ad(&'a [i32]),
    /// Freebayes `RO` and `AO` (`AD` not declared); `None` for a negative `nret`.
    RoAo(Option<&'a [i32]>, Option<&'a [i32]>),
    /// Any other `AD` return code: every depth reads -1.
    Unknown,
}

/// `(Variant.gt_ref_depths, Variant.gt_alt_depths)` for all samples.
pub fn ref_alt_depths(src: DepthSource, n_samples: usize) -> Result<(Vec<i32>, Vec<i32>), String> {
    let unknown = || (vec![-1; n_samples], vec![-1; n_samples]);
    match src {
        DepthSource::Unknown => Ok(unknown()),
        DepthSource::Ad(ad) => {
            let nper = ad.len() / n_samples;
            if nper == 1 {
                return Ok(unknown());
            }
            let mut r = Vec::with_capacity(n_samples);
            let mut a = Vec::with_capacity(n_samples);
            for s in 0..n_samples {
                let i = s * nper;
                r.push(ad[i]);
                // C `int` sum, overflow included: two missing values wrap around to 0.
                let mut alt = ad[i + 1];
                for k in 2..nper {
                    alt = alt.wrapping_add(ad[i + k]);
                }
                a.push(alt);
            }
            Ok((neg_to_minus_one(r), neg_to_minus_one(a)))
        }
        DepthSource::RoAo(ro, ao) => {
            let r = match ro {
                // RO is read as one value per sample, without looking at its length.
                Some(ro) => neg_to_minus_one(ro.iter().take(n_samples).copied().collect()),
                None => vec![-1; n_samples],
            };
            let a = match ao {
                Some(ao) => {
                    let nper = ao.len() / n_samples;
                    if nper == 0 {
                        return Err("FORMAT/AO has fewer values than samples".into());
                    }
                    let mut out = Vec::with_capacity(n_samples);
                    for s in 0..n_samples {
                        let i = s * nper;
                        let mut alt = ao[i];
                        for k in 1..nper {
                            alt = alt.wrapping_add(ao[i + k]);
                        }
                        out.push(alt);
                    }
                    neg_to_minus_one(out)
                }
                None => vec![-1; n_samples],
            };
            Ok((r, a))
        }
    }
}

fn neg_to_minus_one(mut v: Vec<i32>) -> Vec<i32> {
    for x in v.iter_mut() {
        if *x < 0 {
            *x = -1;
        }
    }
    v
}

/// `Variant.gt_depths[i]`: ref + alt, each negative read as 0, -1 when both are.
pub fn depth(r: i32, a: i32) -> i32 {
    if r < 0 && a < 0 {
        -1
    } else {
        r.max(0).wrapping_add(a.max(0))
    }
}

/// `Variant.gt_alt_freqs[i]`.
pub fn alt_freq(total: i32, alt: i32) -> f64 {
    if total < 0 {
        -1.0
    } else if alt < 0 || total == 0 {
        0.0
    } else {
        alt as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // htslib encoding: (allele + 1) << 1 | phased; 0 = missing allele.
    fn enc(a: i32, phased: bool) -> i32 {
        ((a + 1) << 1) | phased as i32
    }

    #[test]
    fn diploid_types() {
        let gts = [
            enc(0, false),
            enc(1, false),
            enc(1, false),
            enc(1, true),
            0,
            enc(1, false),
            0,
            0,
        ];
        assert_eq!(gt_type(&gts, 2, 0), HET);
        assert_eq!(gt_type(&gts, 2, 1), HOM_ALT);
        assert_eq!(gt_type(&gts, 2, 2), UNKNOWN); // ./1 under strict_gt
        assert_eq!(gt_type(&gts, 2, 3), UNKNOWN);
        assert_eq!(calls(&gts, 2, 2), vec![-1, 1]);
        assert!(phased(&gts, 2, 1));
        assert!(!phased(&gts, 2, 0));
    }

    #[test]
    fn haploid_padding() {
        let gts = [enc(1, false), INT32_VECTOR_END, enc(0, false), enc(1, false)];
        assert_eq!(calls(&gts, 2, 0), vec![1]);
        assert_eq!(gt_type(&gts, 2, 0), HOM_ALT);
    }

    #[test]
    fn ad_sums_wrap_like_c() {
        let ad = [10, 5, i32::MIN, i32::MIN];
        let (r, a) = ref_alt_depths(DepthSource::Ad(&ad), 2).unwrap();
        assert_eq!(r, vec![10, -1]);
        assert_eq!(a, vec![5, -1]);
        let ad3 = [1, i32::MIN, i32::MIN];
        let (_, a) = ref_alt_depths(DepthSource::Ad(&ad3), 1).unwrap();
        assert_eq!(a, vec![0]);
    }

    #[test]
    fn freqs() {
        assert_eq!(alt_freq(-1, 3), -1.0);
        assert_eq!(alt_freq(0, 0), 0.0);
        assert_eq!(alt_freq(10, -1), 0.0);
        assert_eq!(alt_freq(10, 5), 0.5);
        assert_eq!(depth(-1, -1), -1);
        assert_eq!(depth(-1, 4), 4);
    }
}
