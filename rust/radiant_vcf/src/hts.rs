//! Thin, cyvcf2-compatible accessors over the htslib C API.
//!
//! rust-htslib gives decoded values (e.g. `GenotypeAllele`), but the Python extraction depends on
//! cyvcf2's view of the *raw* htslib buffers: the missing and vector-end sentinels, the return codes
//! of `bcf_get_format_values`, the INFO scalar rules. These helpers read the same buffers the same way.
//! Ref: cyvcf2 v0.31.1 `cyvcf2/cyvcf2.pyx` (INFO._getval, Variant.format, Variant.QUAL/FILTER/ID).

use std::ffi::{CStr, CString};
use std::os::raw::{c_int, c_void};
use std::ptr;

use rust_htslib::htslib;

/// `bcf_int32_vector_end` (htslib/vcf.h).
pub const INT32_VECTOR_END: i32 = i32::MIN + 1;
const FLOAT_MISSING_BITS: u32 = 0x7F80_0001;

pub fn float_is_missing(f: f32) -> bool {
    f.to_bits() == FLOAT_MISSING_BITS
}

/// A buffer htslib owns and grows with `realloc`, reused across records like cyvcf2's per-record arrays.
pub struct I32Buf {
    ptr: *mut i32,
    cap: c_int,
}

impl I32Buf {
    pub fn new() -> Self {
        Self {
            ptr: ptr::null_mut(),
            cap: 0,
        }
    }

    /// `bcf_get_format_int32`. Returns htslib's `nret`: the number of values, or a negative code
    /// (-1 tag not in header, -2 type mismatch, -3 tag not in record).
    ///
    /// # Safety
    /// `hdr` and `rec` must be valid, `rec` unpacked.
    pub unsafe fn get_format_int32(
        &mut self,
        hdr: *mut htslib::bcf_hdr_t,
        rec: *mut htslib::bcf1_t,
        tag: &CStr,
    ) -> i32 {
        let mut p = self.ptr as *mut c_void;
        let n = unsafe {
            htslib::bcf_get_format_values(
                hdr,
                rec,
                tag.as_ptr(),
                &mut p,
                &mut self.cap,
                htslib::BCF_HT_INT as c_int,
            )
        };
        self.ptr = p as *mut i32;
        n
    }

    pub fn values(&self, n: i32) -> &[i32] {
        if n <= 0 || self.ptr.is_null() {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, n as usize) }
        }
    }
}

/// `I32Buf` for Float FORMAT fields (`bcf_get_format_float`). Missing and padding values keep htslib's NaN bit
/// patterns, as cyvcf2's `format()` returns them.
pub struct F32Buf {
    ptr: *mut f32,
    cap: c_int,
}

impl F32Buf {
    pub fn new() -> Self {
        Self {
            ptr: ptr::null_mut(),
            cap: 0,
        }
    }

    /// # Safety
    /// `hdr` and `rec` must be valid, `rec` unpacked.
    pub unsafe fn get_format_float(
        &mut self,
        hdr: *mut htslib::bcf_hdr_t,
        rec: *mut htslib::bcf1_t,
        tag: &CStr,
    ) -> i32 {
        let mut p = self.ptr as *mut c_void;
        let n = unsafe {
            htslib::bcf_get_format_values(
                hdr,
                rec,
                tag.as_ptr(),
                &mut p,
                &mut self.cap,
                htslib::BCF_HT_REAL as c_int,
            )
        };
        self.ptr = p as *mut f32;
        n
    }

    pub fn values(&self, n: i32) -> &[f32] {
        if n <= 0 || self.ptr.is_null() {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, n as usize) }
        }
    }
}

impl Drop for F32Buf {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { htslib::free(self.ptr as *mut c_void) };
        }
    }
}

impl Drop for I32Buf {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { htslib::free(self.ptr as *mut c_void) };
        }
    }
}

/// A Python value as cyvcf2's `INFO.get` returns it, reduced to what the extraction can store.
#[derive(Debug, Clone, PartialEq)]
pub enum InfoValue {
    Int(i64),
    Float(f32),
    Str(String),
    Bool(bool),
    /// A multi-valued numeric field (a Python list). No scalar column accepts it.
    List,
}

impl InfoValue {
    /// `str(value)` as the slivar helpers use it, for the value types a slivar tag can have.
    pub fn as_py_str(&self) -> Option<String> {
        match self {
            InfoValue::Str(s) => Some(s.clone()),
            InfoValue::Bool(b) => Some(if *b { "True".into() } else { "False".into() }),
            InfoValue::Int(i) => Some(i.to_string()),
            _ => None,
        }
    }
}

/// `record.INFO.get(key)`. `None` for an absent key and for a value cyvcf2 reads as None.
///
/// # Safety
/// `hdr` and `rec` must be valid, `rec` unpacked.
pub unsafe fn get_info(
    hdr: *mut htslib::bcf_hdr_t,
    rec: *mut htslib::bcf1_t,
    key: &CStr,
    is_flag: bool,
) -> Option<InfoValue> {
    let info = unsafe { htslib::bcf_get_info(hdr, rec, key.as_ptr()) };
    if info.is_null() {
        return None;
    }
    let info = unsafe { &*info };
    let typ = info.type_ as u32;
    if info.len == 1 {
        let i = unsafe { info.v1.i };
        match typ {
            htslib::BCF_BT_INT8 => {
                return if i == i8::MIN as i64 {
                    None
                } else {
                    Some(InfoValue::Int(i))
                };
            }
            htslib::BCF_BT_INT16 => {
                return if i == i16::MIN as i64 {
                    None
                } else {
                    Some(InfoValue::Int(i))
                };
            }
            htslib::BCF_BT_INT32 => {
                return if i == i32::MIN as i64 {
                    None
                } else {
                    Some(InfoValue::Int(i))
                };
            }
            htslib::BCF_BT_FLOAT => {
                let f = unsafe { info.v1.f };
                return if float_is_missing(f) {
                    None
                } else {
                    Some(InfoValue::Float(f))
                };
            }
            _ => {}
        }
    }
    if typ == htslib::BCF_BT_CHAR {
        let bytes: &[u8] = if info.vptr.is_null() || info.vptr_len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(info.vptr, info.vptr_len as usize) }
        };
        if !bytes.is_empty() && bytes[0] == 0x7 {
            return None;
        }
        return Some(InfoValue::Str(String::from_utf8_lossy(bytes).into_owned()));
    }
    if info.len == 0 {
        return Some(InfoValue::Bool(is_flag));
    }
    // `bcf_array_to_object`: a list, None only when no value precedes the vector end.
    if info.vptr.is_null() {
        return None;
    }
    let first_is_end = match typ {
        htslib::BCF_BT_INT8 => (unsafe { *(info.vptr as *const i8) }) == i8::MIN + 1,
        htslib::BCF_BT_INT16 => (unsafe { *(info.vptr as *const i16) }) == i16::MIN + 1,
        htslib::BCF_BT_INT32 => (unsafe { *(info.vptr as *const i32) }) == INT32_VECTOR_END,
        htslib::BCF_BT_FLOAT => (unsafe { *(info.vptr as *const f32) }).to_bits() == 0x7F80_0002,
        _ => false,
    };
    if first_is_end { None } else { Some(InfoValue::List) }
}

/// `bcf_get_fmt(hdr, rec, tag) != NULL`: the record carries this FORMAT key.
///
/// # Safety
/// `hdr` and `rec` must be valid.
pub unsafe fn has_format(hdr: *mut htslib::bcf_hdr_t, rec: *mut htslib::bcf1_t, tag: &CStr) -> bool {
    !unsafe { htslib::bcf_get_fmt(hdr, rec, tag.as_ptr()) }.is_null()
}

/// `Variant.QUAL`.
pub fn qual(rec: &htslib::bcf1_t) -> Option<f32> {
    if float_is_missing(rec.qual) {
        None
    } else {
        Some(rec.qual)
    }
}

/// `Variant.FILTER`: None for PASS or `.`, the `;`-joined names otherwise.
///
/// # Safety
/// `hdr` and `rec` must be valid, `rec` unpacked.
pub unsafe fn filter(hdr: *mut htslib::bcf_hdr_t, rec: &htslib::bcf1_t) -> Option<String> {
    let n = rec.d.n_flt as usize;
    if n == 0 {
        return None;
    }
    let ids = unsafe { std::slice::from_raw_parts(rec.d.flt, n) };
    let names: Vec<String> = ids.iter().map(|&id| unsafe { id_name(hdr, id) }).collect();
    if n == 1 && names[0] == "PASS" {
        return None;
    }
    Some(names.join(";"))
}

/// `bcf_hdr_int2id(hdr, BCF_DT_ID, id)`.
unsafe fn id_name(hdr: *mut htslib::bcf_hdr_t, id: c_int) -> String {
    let h = unsafe { &*hdr };
    let pair = unsafe { &*h.id[htslib::BCF_DT_ID as usize].add(id as usize) };
    unsafe { CStr::from_ptr(pair.key) }.to_string_lossy().into_owned()
}

/// `Variant.ID`: None for `.`.
pub fn id(rec: &htslib::bcf1_t) -> Option<String> {
    if rec.d.id.is_null() {
        return None;
    }
    let s = unsafe { CStr::from_ptr(rec.d.id) }.to_string_lossy();
    if s == "." { None } else { Some(s.into_owned()) }
}

/// `Variant.REF` and `Variant.ALT`.
pub fn alleles(rec: &htslib::bcf1_t) -> Vec<String> {
    let n = rec.n_allele() as usize;
    (0..n)
        .map(|i| {
            unsafe { CStr::from_ptr(*rec.d.allele.add(i)) }
                .to_string_lossy()
                .into_owned()
        })
        .collect()
}

pub fn cstring(s: &str) -> CString {
    CString::new(s).expect("tag without NUL")
}
