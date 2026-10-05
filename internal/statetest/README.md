# Model-Driven State Machine Tests

`internal/statetest` is a black-box, model-driven test suite for the public
state machine. It drives the **real** HTTP handler stack
(`bootstrap` → `httpapi` → `assurance` → copy-on-write `repository`) through a
real TCP `httptest.Server`, never the domain functions directly.

## Run

From a clean checkout, with Go installed:

```bash
go test ./...
# verbose, with the race detector
go test -race ./internal/statetest/ -v
# minimal reproduction of the single known production deviation
go test ./internal/statetest/ -run TestSealCommitTimestampAtomicity -v
```

Every scenario is deterministic: the operation sequences come from
`math/rand` sources seeded with fixed constants
(`1, 2, 3, 7, 42, 1009` for the long sequences; four-digit seeds for the
focused scenarios). The only non-reproducible values are wall-clock
timestamps, which the suite never predicts; it checks timestamp *relations*
instead of values.

## What the suite proves

1. **Interleaved requests (linearizability).** A concurrent batch is released
   from one barrier so the requests genuinely race on the repository mutex.
   The suite enumerates every serialization of the batch on a copy of an
   independent oracle model and requires that at least one serialization (a)
   predicts each observed HTTP status and stable error code and (b) ends in
   exactly the state the live service reports afterwards. Covered races:
   double/triple seal, valid-vs-invalid seal, two measurements, valid-vs-invalid
   reopen, two case openings, and a six-request cross-device mixed batch.
2. **Terminal-state retries.** Every mutating verb is replayed against each
   non-current/terminal state; each must be a clean 4xx and must leave the
   detail and event reads byte-for-byte (structurally) unchanged. A duplicate
   reopen right after the legal reopen is included.
3. **Rejected / rolled-back mutations leave no half state.** For every
   rejected attempt the suite snapshots the case view before the attempt, then
   re-reads both `GET /cases/{id}` and `GET /cases/{id}/events` and requires
   structural equality with the snapshot: no version bump, no measurement, no
   event, no seal, no consumed identifier. The two service methods that mutate
   a cloned aggregate *before* validating the request body
   (`ReopenCase` and `SealCase`) get dedicated probes because their correctness
   depends entirely on the repository discarding the clone.
4. **Only successful changes persist.** The oracle reproduces the identifier
   counters; every successful response must carry the exact next id the model
   predicts, so an id burned inside a rejected commit desyncs a later read.
5. **State-machine legality, independent of the oracle.** Every read-back event
   walk is replayed against the documented transition graph; version must equal
   the event count; a sealed case must reference the minimum-deviation accepted
   measurement; at most one unfinished case per device; one seal per case.
6. **Determinism.** A fixed seed rerun produces identical clock-independent
   facts (states, versions, ids, acceptance flags, event kinds and order).

## Design notes

- The oracle (`model_test.go`) is written from `PROJECT_SPEC.md` and the wire
  contracts, not by importing production logic beyond the state/error constant
  names. It even reproduces the production error-precedence quirks (for example,
  `active_case_exists` is checked before tolerance validation when opening a
  case), so precedence regressions show up as status/code mismatches.
- Randomness is drawn once per step into a fixed-size pool. Executors only read
  the pool; they never draw directly, so the number of draws cannot depend on a
  live response (which would desync a fixed seed the moment one request was
  rejected).
- `nil` slices are preserved during timestamp normalization so a serialized
  `"measurements": null` stays distinct from `[]` - the difference that exposes
  a partially constructed response.

## Known production deviation (reported, not worked around)

The suite currently fails on exactly one production behavior; the assertions
are intentionally left in place rather than weakened:

- **Sealing is not stamped as one atomic instant.** See
  `TestSealCommitTimestampAtomicity` for the report and minimal reproduction.
  In `internal/assurance/service.go`, `SealCase` calls `s.now()` three times
  (`item.Seal`, `domain.NewEvidenceSeal`, `domain.NewAuditEvent`), so
  `case.sealed_at`, `seal.sealed_at` and the `case_sealed` event's
  `created_at` are three instants, typically ~0.5–2µs apart on this machine.
  All other workflows stamp one instant per commit. The structural invariants
  (exactly one seal, one seal event, one version bump, correct measurement
  selection) all hold; only same-commit timestamp equality is violated.
