//! C-ABI shim used only by the Lean verification harness (mvsqlite-verify/).
//! Exposes real decision functions from `mvsqlite-core` used in production
//! commit.rs, so the Lean witness search exercises the actual compiled
//! decision logic rather than a hand-transcribed model of it.

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

/// Returns the real `mvsqlite_core::commit::decide_idempotency_check`'s
/// decision as a discriminant: 0 = Proceed, 1 = Conflict, 2 =
/// AlreadyCommitted. The specific version an AlreadyCommitted decision would
/// return is always exactly `existing_idempotency_record_version` (see that
/// function's body - it returns the record's own version verbatim), so the
/// Lean model checks that equality itself rather than this shim echoing it
/// back.
///
/// Each 10-byte version is passed as an 8-byte high part plus a 2-byte low
/// part, matching `mv_version_conflicts`'s convention - plain scalar
/// arguments, no ByteArray marshalling from Lean's `@[extern]` FFI. Unlike
/// the old (buggy) design, there is no idempotency token to compare here at
/// all: `has_existing_idempotency_record` alone is the signal, since that
/// key is unique per idempotency_key by construction (see
/// `KeyCodec::construct_idempotency_record_key`) - its mere presence
/// unconditionally means this specific attempt already committed.
#[no_mangle]
pub extern "C" fn mv_decide_idempotency_check(
    maybe_committed: u8,
    plcc_enable_ns: u8,
    has_existing_idempotency_record: u8,
    existing_idempotency_record_version_hi: u64,
    existing_idempotency_record_version_lo: u16,
    has_existing_lwv: u8,
    existing_lwv_hi: u64,
    existing_lwv_lo: u16,
    client_assumed_version_hi: u64,
    client_assumed_version_lo: u16,
) -> u8 {
    let existing_idempotency_record = if has_existing_idempotency_record != 0 {
        Some(version_from_parts(
            existing_idempotency_record_version_hi,
            existing_idempotency_record_version_lo,
        ))
    } else {
        None
    };
    let existing_last_write_version = if has_existing_lwv != 0 {
        Some(version_from_parts(existing_lwv_hi, existing_lwv_lo))
    } else {
        None
    };
    let client_assumed_version =
        version_from_parts(client_assumed_version_hi, client_assumed_version_lo);

    match mvsqlite_core::commit::decide_idempotency_check(
        maybe_committed != 0,
        plcc_enable_ns != 0,
        existing_idempotency_record,
        existing_last_write_version,
        client_assumed_version,
    ) {
        mvsqlite_core::commit::IdempotencyDecision::Proceed => 0,
        mvsqlite_core::commit::IdempotencyDecision::Conflict => 1,
        mvsqlite_core::commit::IdempotencyDecision::AlreadyCommitted(_) => 2,
    }
}
