import PlausibleWitnessDag

/-! # mvsqlite commit-protocol safety check

Uses `plausible-witness-dag` as the search driver, backed by the REAL compiled
`mvsqlite_core::util::version_conflicts` function (via a static-lib FFI shim,
`mvsqlite-verify-ffi`) as the conflict-decision oracle — not a hand-written
reimplementation of it.

Property under test: for two concurrently-started transactions T1, T2 that
each read-then-write a set of pages (read_set ⊇ write_set, the common SQLite
case), if their write sets overlap, the second one to commit must be reported
as conflicting. A "witness" is a scenario where that fails — i.e. a lost
update / write-skew hole in the real conflict-decision logic.

This exhaustively covers all `w1, w2 ⊆ {page0, page1, page2}` (64 scenarios),
so a `provablyNone` result at L0 is a genuine (bounded) verification result
for this scenario class, not a sampled one.
-/

namespace MvsqliteVerify

open PlausibleWitnessDag

/-- FFI: the real conflict-decision function from `mvsqlite-core`, linked via
`mvsqlite-verify-ffi`'s static lib (see `../mvsqlite-verify-ffi/src/lib.rs`).
Each 10-byte version is split into an 8-byte high part and a 2-byte low part
so this maps directly to Lean's scalar FFI with no ByteArray marshalling. -/
@[extern "mv_version_conflicts"]
opaque versionConflictsFFI (currentHi : UInt64) (currentLo : UInt16)
    (observedHi : UInt64) (observedLo : UInt16) : UInt8

def v0 : UInt64 × UInt16 := (0, 0)
def v1 : UInt64 × UInt16 := (1, 0)

/-- Wrapper calling the real FFI-linked decision function. -/
def conflicts (current observed : UInt64 × UInt16) : Bool :=
  versionConflictsFFI current.1 current.2 observed.1 observed.2 != 0

/-- A write-set over 3 modeled pages, as a bitmask. -/
abbrev PageSet := Nat

def numPages : Nat := 3

def hasPage (s : PageSet) (i : Nat) : Bool :=
  (s / (2 ^ i)) % 2 == 1

def overlap (a b : PageSet) : Bool :=
  (List.range numPages).any (fun i => hasPage a i && hasPage b i)

/-- Simulate T1 (writes `w1`) committing first, then T2 (writes `w2`)
attempting to commit, both having read every page they write at `v0`. Every
page T1 writes moves to `v1`; T2's per-page conflict check then runs through
the REAL `conflicts` (FFI) function, exactly as `commit.rs`'s PLCC loop does
per read-set page. Returns `true` iff this is an unsafe witness: the write
sets overlap, yet T2's commit was not flagged as conflicting anywhere. -/
def isUnsafeWitness (w1 w2 : PageSet) : Bool :=
  let versionAfterT1 (p : Nat) : UInt64 × UInt16 := if hasPage w1 p then v1 else v0
  let t2SeesConflict :=
    (List.range numPages).any (fun p => hasPage w2 p && conflicts (versionAfterT1 p) v0)
  overlap w1 w2 && !t2SeesConflict

def decodeCandidate (c : Nat) : PageSet × PageSet :=
  (c % 8, (c / 8) % 8)

/-- Plausible-facing candidate predicate (used for `certify`'s own randomized
cross-check; the authoritative search is `readback` below). -/
def candidateIsWitness (_lvl : Level) (c : Nat) : Bool :=
  let (w1, w2) := decodeCandidate c
  isUnsafeWitness w1 w2

/-- Deterministic, exhaustive readback: the full candidate space (64 scenarios
of `w1, w2 ⊆ {0,1,2}`) fits comfortably in one pass regardless of walk-step
budget, so this is a complete search, not a truncated walk. -/
def readback (_steps : Nat) : Readback (Option (PageSet × PageSet)) :=
  match (List.range 64).find? (fun c => let (w1, w2) := decodeCandidate c; isUnsafeWitness w1 w2) with
  | some c =>
      { value := some (decodeCandidate c), found := true, witnessIdx := c, budgetHit := false }
  | none =>
      { value := none, found := false, witnessIdx := 0, budgetHit := false }

def runSafetyCheck : IO (Option (PageSet × PageSet) × Nat × TraceEntry) :=
  resolve "overlapping-write commits must conflict (FFI-backed)" candidateIsWitness readback

/-! ## Phantom-commit / idempotency-check safety

Root cause of a real bug found the night this was written: a retry after an
*ambiguous* FDB commit error (the `commit_unknown_result` class - the client
genuinely cannot tell whether the previous attempt landed) must check
whether that previous attempt already committed before trying again.
`mvclient` used to skip that check on every attempt at all.

The first fix made `Core::commit` check FDB's own `maybe_committed` retry
flag against the namespace's *last-write-version* key - but that key is a
single mutable pointer shared by every commit to the namespace. A second,
genuinely concurrent commit landing between an ambiguous attempt and its
retry silently overwrites it, so the retry ends up asking "who wrote here
most recently" instead of "did *my* attempt commit" - the same bug in a
narrower, harder-to-hit shape. The actual fix
(`KeyCodec::construct_idempotency_record_key`) uses a dedicated point-lookup
key unique per idempotency_key, which no other transaction's commit can
ever touch.

This section exhaustively checks the corrected property against the real
compiled `mvsqlite_core::commit::decide_idempotency_check` - not a
hand-transcribed reimplementation of it - via `mvsqlite-verify-ffi`'s
`mv_decide_idempotency_check`.

Two independent properties, checked together:
1. Whenever a retry is flagged `maybe_committed` AND this transaction's own
   idempotency record already exists, the decision must be
   `AlreadyCommitted` - never `Proceed` (a second, real, untracked commit -
   the phantom commit) and never `Conflict` (wrongly discarding a commit
   that actually succeeded) - regardless of PLCC or the unrelated LWV
   state.
2. Whenever that's *not* the case, PLCC is disabled, and the existing LWV
   conflicts with what the client assumed, the decision must be `Conflict`
   - the original coarse conflict check, now fully decoupled from
   idempotency, must still work. -/

/-- FFI: the real idempotency/conflict decision function from
`mvsqlite-core::commit`. Returns 0 = Proceed, 1 = Conflict,
2 = AlreadyCommitted. See `mvsqlite-verify-ffi/src/lib.rs` for the exact
scalar marshalling convention (each 10-byte version as `UInt64` high +
`UInt16` low, matching `versionConflictsFFI` above - there is no token to
marshal at all anymore, since presence of the dedicated record is itself
the signal). -/
@[extern "mv_decide_idempotency_check"]
opaque decideIdempotencyCheckFFI
    (maybeCommitted plccEnableNs hasExistingIdempotencyRecord : UInt8)
    (existingIdempotencyRecordVersionHi : UInt64) (existingIdempotencyRecordVersionLo : UInt16)
    (hasExistingLwv : UInt8)
    (existingLwvHi : UInt64) (existingLwvLo : UInt16)
    (clientAssumedVersionHi : UInt64) (clientAssumedVersionLo : UInt16) : UInt8

def boolToU8 (b : Bool) : UInt8 := if b then 1 else 0

def alreadyCommittedDiscriminant : UInt8 := 2
def conflictDiscriminant : UInt8 := 1

/-- Candidate space: is this a retry after an ambiguous error
(`maybeCommitted`), is PLCC enabled for this namespace, does this
transaction's own idempotency record already exist, does a (necessarily
unrelated, since the record key is unique per idempotency_key) LWV entry
exist, and does that LWV conflict with what the client assumed (using the
same `v0`/`v1` representatives as the PLCC check above -
`decide_idempotency_check` only ever compares versions via the
already-separately-verified `conflicts` function, so two representative
points fully cover its behavior). -/
def decodeIdempotencyCandidate (c : Nat) : Bool × Bool × Bool × Bool × Bool :=
  (c % 2 == 1, (c / 2) % 2 == 1, (c / 4) % 2 == 1, (c / 8) % 2 == 1, (c / 16) % 2 == 1)

def callDecide (maybeCommitted plccEnableNs hasIdemRecord hasLwv versionConflictsCase : Bool) : UInt8 :=
  let (idemVerHi, idemVerLo) := v1
  let (lwvHi, lwvLo) := if versionConflictsCase then v1 else v0
  let (assumedHi, assumedLo) := v0
  decideIdempotencyCheckFFI
    (boolToU8 maybeCommitted) (boolToU8 plccEnableNs) (boolToU8 hasIdemRecord)
    idemVerHi idemVerLo
    (boolToU8 hasLwv) lwvHi lwvLo
    assumedHi assumedLo

/-- `true` iff this is an unsafe witness for either of the two properties
above. -/
def isUnsafeIdempotencyWitness (c : Nat) : Bool :=
  let (maybeCommitted, plccEnableNs, hasIdemRecord, hasLwv, versionConflictsCase) :=
    decodeIdempotencyCandidate c
  let decision := callDecide maybeCommitted plccEnableNs hasIdemRecord hasLwv versionConflictsCase
  if maybeCommitted && hasIdemRecord then
    decision != alreadyCommittedDiscriminant
  else
    !plccEnableNs && hasLwv && versionConflictsCase && decision != conflictDiscriminant

def candidateIsIdempotencyWitness (_lvl : Level) (c : Nat) : Bool :=
  isUnsafeIdempotencyWitness c

/-- Exhaustive over all 32 combinations of (maybeCommitted, plccEnableNs,
hasIdemRecord, hasLwv, versionConflictsCase) - small enough for a complete
pass regardless of walk-step budget. -/
def idempotencyReadback (_steps : Nat) : Readback (Option Nat) :=
  match (List.range 32).find? isUnsafeIdempotencyWitness with
  | some c => { value := some c, found := true, witnessIdx := c, budgetHit := false }
  | none => { value := none, found := false, witnessIdx := 0, budgetHit := false }

def runIdempotencySafetyCheck : IO (Option Nat × Nat × TraceEntry) :=
  resolve "retry after ambiguous commit must discover its own prior success (FFI-backed)"
    candidateIsIdempotencyWitness idempotencyReadback

def main (_args : List String) : IO Unit := do
  let (witness, lvl, trace) ← runSafetyCheck
  IO.println s!"resolved level: L{lvl}"
  IO.println s!"trace: {repr trace}"
  match witness with
  | some (w1, w2) =>
      IO.println s!"UNSAFE WITNESS FOUND: w1={w1} w2={w2} (bitmasks over 3 pages)"
      IO.println "The real mvsqlite-core conflict-decision function failed to detect an overlapping write."
      throw <| IO.userError "commit-protocol safety property violated"
  | none =>
      IO.println
        "No witness found across all 64 modeled (w1, w2) scenarios: every overlapping-write \
         case was correctly flagged as conflicting by the real mvsqlite-core FFI function."

  let (idemWitness, idemLvl, idemTrace) ← runIdempotencySafetyCheck
  IO.println s!"resolved level: L{idemLvl}"
  IO.println s!"trace: {repr idemTrace}"
  match idemWitness with
  | some c =>
      let (maybeCommitted, plccEnableNs, hasIdemRecord, hasLwv, versionConflictsCase) :=
        decodeIdempotencyCandidate c
      IO.println s!"UNSAFE WITNESS FOUND: maybeCommitted={maybeCommitted} plccEnableNs={plccEnableNs} \
        hasIdemRecord={hasIdemRecord} hasLwv={hasLwv} versionConflictsCase={versionConflictsCase}"
      IO.println "The real mvsqlite-core idempotency-check function either failed to discover a \
        prior successful commit on retry (the phantom-commit bug class), or failed to still \
        detect an ordinary non-PLCC conflict."
      throw <| IO.userError "phantom-commit / conflict-detection safety property violated"
  | none =>
      IO.println
        "No witness found across all 32 modeled scenarios: every retry-after-ambiguous-error \
         case where this transaction's own idempotency record already exists was correctly \
         reported as AlreadyCommitted, and every remaining non-PLCC LWV conflict was still \
         correctly reported as Conflict, by the real mvsqlite-core FFI function."

end MvsqliteVerify

def main (args : List String) : IO Unit := MvsqliteVerify.main args
