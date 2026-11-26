//! Port of the parental origin and transmission mode of `radiant/tasks/vcf/snv/germline/occurrence.py`
//! (ETL rules) and `radiant/tasks/vcf/snv/germline/inheritance.py` (slivar tags, RAD-93).

use crate::tables::*;

/// `normalize_calls`: sorted.
pub fn normalize_calls(calls: &[i32]) -> Vec<i32> {
    let mut v = calls.to_vec();
    v.sort_unstable();
    v
}

fn normalize_monosomy(calls: &[i32]) -> Vec<i32> {
    if calls.len() == 1 {
        vec![calls[0], calls[0]]
    } else {
        calls.to_vec()
    }
}

const UNKNOWN_PARENT: &[i32] = &[-1, -1];

/// `parental_origin`.
pub fn parental_origin(
    chromosome: &str,
    progeny: &[i32],
    father: Option<&[i32]>,
    mother: Option<&[i32]>,
) -> &'static str {
    if father.is_none() && mother.is_none() {
        return "UNKNOWN";
    }
    // Python `x if x else (-1, -1)`: an empty tuple also reads as unknown.
    let f = father.filter(|c| !c.is_empty()).unwrap_or(UNKNOWN_PARENT);
    let m = mother.filter(|c| !c.is_empty()).unwrap_or(UNKNOWN_PARENT);
    let table = match chromosome {
        "Y" => Y_ORIGINS_LOOKUP,
        "X" => X_ORIGINS_LOOKUP,
        _ => AUTOSOMAL_ORIGINS_LOOKUP,
    };
    table
        .iter()
        .find(|(p, tf, tm, _)| *p == progeny && *tf == f && *tm == m)
        .map(|e| e.3)
        .unwrap_or("UNKNOWN")
}

/// `compute_transmission_mode`.
pub fn transmission_mode(
    chromosome: &str,
    sex: &str,
    progeny: &[i32],
    father: Option<&[i32]>,
    mother: Option<&[i32]>,
    father_affected: bool,
    mother_affected: bool,
) -> Option<&'static str> {
    let (father, mother) = match (father, mother) {
        (None, None) => return Some("unknown_parents_genotype"),
        (None, _) => return Some("unknown_father_genotype"),
        (_, None) => return Some("unknown_mother_genotype"),
        (Some(f), Some(m)) => (f, m),
    };
    let c = normalize_monosomy(progeny);
    let f = normalize_monosomy(father);
    let m = normalize_monosomy(mother);
    if c == [0, 0] {
        return Some("non_carrier_proband");
    }
    if c == [-1, -1] {
        return Some("unknown_proband_genotype");
    }
    if chromosome == "X" || chromosome == "Y" {
        SEXUAL_TRANSMISSION_LOOKUP
            .iter()
            .find(|e| {
                e.0 == sex && e.1 == c && e.2 == f && e.3 == m && e.4 == father_affected && e.5 == mother_affected
            })
            .map(|e| e.6)
    } else {
        AUTOSOMAL_TRANSMISSION_LOOKUP
            .iter()
            .find(|e| e.0 == c && e.1 == f && e.2 == m && e.3 == father_affected && e.4 == mother_affected)
            .map(|e| e.5)
    }
}

pub const PO_TAGS: &[(&str, &str)] = &[
    ("po_denovo", "DENOVO"),
    ("po_mother", "MOTHER"),
    ("po_father", "FATHER"),
    ("po_both", "BOTH"),
    ("po_ambiguous", "AMBIGUOUS"),
    ("po_possible_denovo", "POSSIBLE_DENOVO"),
    ("po_possible_mother", "POSSIBLE_MOTHER"),
    ("po_possible_father", "POSSIBLE_FATHER"),
    ("po_unknown", "UNKNOWN"),
];

pub const MODE_TAGS: &[(&str, &str)] = &[
    ("autosomal_dominant_denovo", "autosomal_dominant_de_novo"),
    ("x_linked_dominant_denovo", "x_linked_dominant_de_novo"),
    ("x_linked_recessive_denovo", "x_linked_recessive_de_novo"),
    ("autosomal_recessive", "autosomal_recessive"),
    ("x_linked_recessive", "x_linked_recessive"),
    ("autosomal_dominant", "autosomal_dominant"),
    ("x_linked_dominant", "x_linked_dominant"),
    ("uniparental_disomy", "uniparental_disomy"),
];

pub const MODE_TAGS_MARKER: &str = "autosomal_dominant_denovo";

pub const GENOTYPE_TAGS: &[&str] = &[
    "unknown_parents_genotype",
    "unknown_father_genotype",
    "unknown_mother_genotype",
    "unknown_proband_genotype",
    "non_carrier_proband",
];

pub const CANDIDATE_TAG: &str = "candidate";

/// The slivar tags of one record, already split, keyed by tag. A tag absent from the record has no entry.
pub struct SlivarTags {
    pub listed: std::collections::HashMap<&'static str, Vec<String>>,
}

impl SlivarTags {
    fn listed(&self, tag: &str, aliquot: &str) -> bool {
        self.listed.get(tag).is_some_and(|v| v.iter().any(|a| a == aliquot))
    }

    /// `slivar_parental_origin`.
    pub fn parental_origin(&self, aliquot: &str) -> Option<&'static str> {
        PO_TAGS
            .iter()
            .find(|(tag, _)| self.listed(tag, aliquot))
            .map(|(_, o)| *o)
    }

    /// `slivar_transmission_mode`: `(mode, other_modes, candidate)`.
    pub fn transmission_mode(&self, aliquot: &str) -> (Option<&'static str>, Vec<&'static str>, bool) {
        let modes: Vec<&'static str> = MODE_TAGS
            .iter()
            .filter(|(tag, _)| self.listed(tag, aliquot))
            .map(|(_, m)| *m)
            .collect();
        let candidate = self.listed(CANDIDATE_TAG, aliquot);
        if let Some((first, rest)) = modes.split_first() {
            return (Some(first), rest.to_vec(), candidate);
        }
        let genotype = GENOTYPE_TAGS.iter().find(|tag| self.listed(tag, aliquot)).copied();
        (genotype, vec![], candidate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origins() {
        assert_eq!(parental_origin("1", &[0, 1], Some(&[0, 0]), Some(&[0, 0])), "DENOVO");
        assert_eq!(parental_origin("1", &[0, 1], None, Some(&[0, 1])), "POSSIBLE_MOTHER");
        assert_eq!(parental_origin("1", &[0, 1], None, None), "UNKNOWN");
        assert_eq!(parental_origin("X", &[1], Some(&[0]), Some(&[0, 1])), "MOTHER");
    }

    #[test]
    fn modes() {
        assert_eq!(
            transmission_mode("1", "Male", &[0, 1], Some(&[0, 0]), Some(&[0, 0]), false, false),
            Some("autosomal_dominant_de_novo")
        );
        assert_eq!(
            transmission_mode("1", "Male", &[0, 0], Some(&[0, 0]), Some(&[0, 0]), false, false),
            Some("non_carrier_proband")
        );
        assert_eq!(
            transmission_mode("X", "Male", &[1], Some(&[0]), Some(&[0, 1]), false, false),
            Some("x_linked_recessive")
        );
        assert_eq!(
            transmission_mode("1", "Male", &[0, 1], None, Some(&[0, 0]), false, false),
            Some("unknown_father_genotype")
        );
    }
}
