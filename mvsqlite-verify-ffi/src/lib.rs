//! C-ABI shim used only by the Lean verification harness (mvsqlite-verify/).
//! Exposes the real `mvsqlite_core::util::version_conflicts` function used in
//! production commit.rs, so the Lean witness search exercises the actual
//! compiled decision logic rather than a hand-transcribed model of it.

fn version_from_parts(hi: u64, lo: u16) -> [u8; 10] {
    let mut out = [0u8; 10];
    out[0..8].copy_from_slice(&hi.to_be_bytes());
    out[8..10].copy_from_slice(&lo.to_be_bytes());
    out
}

/// Returns 1 if `current_version` conflicts with a transaction that observed
/// `observed_version` (i.e. the resource changed after the transaction's
/// read), 0 otherwise. Each 10-byte version is passed as an 8-byte high part
/// plus a 2-byte low part so this can be called with plain scalar arguments
/// from Lean's `@[extern]` FFI without any ByteArray marshalling.
#[no_mangle]
pub extern "C" fn mv_version_conflicts(
    current_hi: u64,
    current_lo: u16,
    observed_hi: u64,
    observed_lo: u16,
) -> u8 {
    let current = version_from_parts(current_hi, current_lo);
    let observed = version_from_parts(observed_hi, observed_lo);
    if mvsqlite_core::util::version_conflicts(current, observed) {
        1
    } else {
        0
    }
}
