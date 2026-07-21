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

end MvsqliteVerify

def main (args : List String) : IO Unit := MvsqliteVerify.main args
