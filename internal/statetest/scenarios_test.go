package statetest_test

import (
	"fmt"
	"net/http"
	"testing"
)

// Fixed seeds make every interleaving, rejected request and generated value
// reproducible from a clean checkout. Running a single scenario with
// -run <name> -v is the minimal reproduction of any reported divergence.
var sequentialSeeds = []int64{1, 2, 3, 7, 42, 1009}

func TestModelDrivenSequential(t *testing.T) {
	for _, seed := range sequentialSeeds {
		seed := seed
		t.Run(fmt.Sprintf("seed=%04d", seed), func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			d := newDriver(t, h, seed)
			d.runSequential(4, 220)
			t.Logf("seed=%d operation mix: %v", seed, d.stats)
		})
	}
}

// setupAwaiting registers a device and opens an awaiting case on the oracle
// and the live service, returning their ids.
func (d *driver) setupAwaiting(tag string) (deviceID, caseID string) {
	d.t.Helper()
	predDev, deviceID := d.m.registerPredict(tag, "Bench Meter", "Room A", "Lin")
	res := d.mustDo(http.MethodPost, "/devices", map[string]any{
		"asset_tag": tag, "model": "Bench Meter", "room": "Room A", "steward": "Lin",
	})
	if res.status != http.StatusCreated || res.device == nil || res.device.ID != deviceID || !predDev.ok {
		d.t.Fatalf("setup device: status=%d body=%s", res.status, res.raw)
	}
	predCase, caseID := d.m.openPredict(deviceID, 25.0, 0.5, "Qiao")
	openRes := d.mustDo(http.MethodPost, "/cases", map[string]any{
		"device_id": deviceID, "nominal": 25.0, "tolerance": 0.5, "steward": "Qiao",
	})
	if openRes.status != http.StatusCreated || openRes.detail == nil || openRes.detail.Case.ID != caseID || !predCase.ok {
		d.t.Fatalf("setup case: status=%d body=%s", openRes.status, openRes.raw)
	}
	return deviceID, caseID
}

// commitMeasurement drives one measurement through the oracle and the service.
func (d *driver) commitMeasurement(caseID string, observed float64, tech string) {
	d.t.Helper()
	pred := d.m.measurementPredict(caseID, observed, tech)
	res := d.mustDo(http.MethodPost, "/cases/"+caseID+"/measurements", map[string]any{
		"observed": observed, "technician": tech,
	})
	if res.status != pred.status || res.code != pred.code {
		d.t.Fatalf("setup measurement: got (%d,%s) want (%d,%s) body=%s",
			res.status, res.code, pred.status, pred.code, res.raw)
	}
}

func (d *driver) commitReopen(caseID, actor, reason string) {
	d.t.Helper()
	pred := d.m.reopenPredict(caseID, actor, reason)
	res := d.mustDo(http.MethodPost, "/cases/"+caseID+"/reopen", map[string]any{
		"actor": actor, "reason": reason,
	})
	if res.status != pred.status || res.code != pred.code {
		d.t.Fatalf("setup reopen: got (%d,%s) want (%d,%s) body=%s",
			res.status, res.code, pred.status, pred.code, res.raw)
	}
}

// TestConcurrentDoubleSeal races several seal requests against a review_ready
// case (the model-driven generalization of the workflowcheck race): exactly one
// may commit a version/seal/event fact, every loser must be a clean conflict,
// and the aggregate outcome must match one total serialization.
func TestConcurrentDoubleSeal(t *testing.T) {
	h := newHarness(t)
	d := newDriver(t, h, 9001)
	_, caseID := d.setupAwaiting("MET-C-SEAL")
	d.commitMeasurement(caseID, 25.2, "Rui")

	reqs := []concRequest{
		{
			name:   "seal-a",
			method: http.MethodPost,
			path:   "/cases/" + caseID + "/seal",
			body:   map[string]any{"reviewer": "Anke", "summary": "seal winner a"},
			apply: func(m *model) outcome {
				return m.sealPredict(caseID, "Anke", "seal winner a")
			},
		},
		{
			name:   "seal-b",
			method: http.MethodPost,
			path:   "/cases/" + caseID + "/seal",
			body:   map[string]any{"reviewer": "Bram", "summary": "seal winner b"},
			apply: func(m *model) outcome {
				return m.sealPredict(caseID, "Bram", "seal winner b")
			},
		},
		{
			name:   "seal-c",
			method: http.MethodPost,
			path:   "/cases/" + caseID + "/seal",
			body:   map[string]any{"reviewer": "Lin", "summary": "seal winner c"},
			apply: func(m *model) outcome {
				return m.sealPredict(caseID, "Lin", "seal winner c")
			},
		},
	}
	d.runConcurrentBatch("double-seal", reqs)

	// Exact winner accounting, independent of which serialization matched.
	final := d.readDetail(caseID)
	if final.Case.State != stSealed || final.Seal == nil {
		t.Fatalf("case did not settle sealed: %+v", final.Case)
	}
	sealEvents := 0
	for _, e := range final.Events {
		if e.Kind == evCaseSealed {
			sealEvents++
		}
	}
	if sealEvents != 1 {
		t.Errorf("exactly one case_sealed event required, observed %d", sealEvents)
	}
	if final.Case.Version != 3 {
		t.Errorf("sealed case version = %d, want 3 (open, measure, seal)", final.Case.Version)
	}
}

// TestConcurrentSealVsInvalidSeal races a valid seal with an invalid-reviewer
// seal. Both serializations are legal (invalid-first => valid seals;
// valid-first => invalid gets seal_exists), and neither may leave a rollback
// artifact: a half-sealed case, an orphan seal id or a missing seal event.
func TestConcurrentSealVsInvalidSeal(t *testing.T) {
	h := newHarness(t)
	d := newDriver(t, h, 9002)
	_, caseID := d.setupAwaiting("MET-C-INVSEAL")
	d.commitMeasurement(caseID, 25.2, "Rui")

	reqs := []concRequest{
		{
			name:   "seal-valid",
			method: http.MethodPost,
			path:   "/cases/" + caseID + "/seal",
			body:   map[string]any{"reviewer": "Anke", "summary": "valid"},
			apply:  func(m *model) outcome { return m.sealPredict(caseID, "Anke", "valid") },
		},
		{
			name:   "seal-invalid",
			method: http.MethodPost,
			path:   "/cases/" + caseID + "/seal",
			body:   map[string]any{"reviewer": "   ", "summary": ""},
			apply:  func(m *model) outcome { return m.sealPredict(caseID, "   ", "") },
		},
	}
	d.runConcurrentBatch("seal-vs-invalid-seal", reqs)
}

// TestConcurrentMeasurements races two measurements against an awaiting case.
// Exactly one may commit; the loser must be rejected by the state guard with
// no measurement/event/version residue.
func TestConcurrentMeasurements(t *testing.T) {
	h := newHarness(t)
	d := newDriver(t, h, 9003)
	_, caseID := d.setupAwaiting("MET-C-MEAS")

	reqs := []concRequest{
		{
			name:   "measure-in-tolerance",
			method: http.MethodPost,
			path:   "/cases/" + caseID + "/measurements",
			body:   map[string]any{"observed": 25.1, "technician": "Rui"},
			apply:  func(m *model) outcome { return m.measurementPredict(caseID, 25.1, "Rui") },
		},
		{
			name:   "measure-out-tolerance",
			method: http.MethodPost,
			path:   "/cases/" + caseID + "/measurements",
			body:   map[string]any{"observed": 26.4, "technician": "Mira"},
			apply:  func(m *model) outcome { return m.measurementPredict(caseID, 26.4, "Mira") },
		},
	}
	d.runConcurrentBatch("double-measurement", reqs)

	final := d.readDetail(caseID)
	if len(final.Measurements) != 1 {
		t.Errorf("exactly one measurement must commit from a race, got %d", len(final.Measurements))
	}
	if final.Case.Version != 2 {
		t.Errorf("version after raced measurement = %d, want 2", final.Case.Version)
	}
}

// TestConcurrentReopenVsInvalidReopen races a valid reopen against an
// invalid-actor reopen on a recheck_required case. The production service moves
// the cloned case back to awaiting_measurement before validating the body, so
// the invalid request only stays correct because the repository rolls the clone
// back; this race proves that rollback path under contention.
func TestConcurrentReopenVsInvalidReopen(t *testing.T) {
	h := newHarness(t)
	d := newDriver(t, h, 9004)
	_, caseID := d.setupAwaiting("MET-C-REOP")
	d.commitMeasurement(caseID, 26.4, "Rui") // -> recheck_required

	reqs := []concRequest{
		{
			name:   "reopen-valid",
			method: http.MethodPost,
			path:   "/cases/" + caseID + "/reopen",
			body:   map[string]any{"actor": "Lin", "reason": "setup replaced"},
			apply:  func(m *model) outcome { return m.reopenPredict(caseID, "Lin", "setup replaced") },
		},
		{
			name:   "reopen-invalid",
			method: http.MethodPost,
			path:   "/cases/" + caseID + "/reopen",
			body:   map[string]any{"actor": "", "reason": ""},
			apply:  func(m *model) outcome { return m.reopenPredict(caseID, "", "") },
		},
	}
	d.runConcurrentBatch("reopen-vs-invalid-reopen", reqs)

	final := d.readDetail(caseID)
	reopenEvents := 0
	for _, e := range final.Events {
		if e.Kind == evCaseReopened {
			reopenEvents++
		}
	}
	if reopenEvents != 1 {
		t.Errorf("exactly one case_reopened event required, got %d", reopenEvents)
	}
}

// TestConcurrentOpenTwice races two case openings for the same active device.
// One case may exist; the loser must be active_case_exists with no orphan
// case/event identifiers consumed.
func TestConcurrentOpenTwice(t *testing.T) {
	h := newHarness(t)
	d := newDriver(t, h, 9005)
	deviceID, _ := d.setupAwaiting("MET-C-OPEN")
	// Close nothing: setupAwaiting already left an unfinished case open on the
	// device, so racing two more openings must all collide. To race *first*
	// openings instead, set up a second fresh device:
	tag := "MET-C-OPEN2"
	predDev, freshDevice := d.m.registerPredict(tag, "Bench Meter", "Room A", "Lin")
	res := d.mustDo(http.MethodPost, "/devices", map[string]any{
		"asset_tag": tag, "model": "Bench Meter", "room": "Room A", "steward": "Lin",
	})
	if !predDev.ok || res.status != http.StatusCreated || res.device == nil || res.device.ID != freshDevice {
		t.Fatalf("fresh device setup failed: %s", res.raw)
	}
	_ = deviceID

	reqs := []concRequest{
		{
			name:   "open-a",
			method: http.MethodPost,
			path:   "/cases",
			body:   map[string]any{"device_id": freshDevice, "nominal": 25.0, "tolerance": 0.5, "steward": "Qiao"},
			apply:  func(m *model) outcome { o, _ := m.openPredict(freshDevice, 25.0, 0.5, "Qiao"); return o },
		},
		{
			name:   "open-b",
			method: http.MethodPost,
			path:   "/cases",
			body:   map[string]any{"device_id": freshDevice, "nominal": 25.0, "tolerance": 0.5, "steward": "Qiao"},
			apply:  func(m *model) outcome { o, _ := m.openPredict(freshDevice, 25.0, 0.5, "Qiao"); return o },
		},
	}
	d.runConcurrentBatch("double-open", reqs)
}

// TestCrossDeviceMixedInterleaving drives several devices whose cases sit in
// different states, then fires a mixed concurrent batch so independent commits
// interleave with conflicting ones. Every case must still match some legal
// serialization.
func TestCrossDeviceMixedInterleaving(t *testing.T) {
	h := newHarness(t)
	d := newDriver(t, h, 9006)

	_, awaiting := d.setupAwaiting("MET-C-MIX-A")
	_, recheck := d.setupAwaiting("MET-C-MIX-B")
	d.commitMeasurement(recheck, 26.4, "Rui")
	_, reviewReady := d.setupAwaiting("MET-C-MIX-C")
	d.commitMeasurement(reviewReady, 25.2, "Rui")
	_, sealed := d.setupAwaiting("MET-C-MIX-D")
	d.commitMeasurement(sealed, 25.2, "Rui")
	d.commitSealForSetup(sealed, "Anke", "setup seal")

	reqs := []concRequest{
		{name: "measure-awaiting", method: http.MethodPost, path: "/cases/" + awaiting + "/measurements",
			body:  map[string]any{"observed": 25.1, "technician": "Rui"},
			apply: func(m *model) outcome { return m.measurementPredict(awaiting, 25.1, "Rui") }},
		{name: "measure-awaiting-again", method: http.MethodPost, path: "/cases/" + awaiting + "/measurements",
			body:  map[string]any{"observed": 25.2, "technician": "Mira"},
			apply: func(m *model) outcome { return m.measurementPredict(awaiting, 25.2, "Mira") }},
		{name: "reopen-recheck", method: http.MethodPost, path: "/cases/" + recheck + "/reopen",
			body:  map[string]any{"actor": "Lin", "reason": "probe replaced"},
			apply: func(m *model) outcome { return m.reopenPredict(recheck, "Lin", "probe replaced") }},
		{name: "measure-recheck-wrong-state", method: http.MethodPost, path: "/cases/" + recheck + "/measurements",
			body:  map[string]any{"observed": 25.0, "technician": "Qiao"},
			apply: func(m *model) outcome { return m.measurementPredict(recheck, 25.0, "Qiao") }},
		{name: "seal-review", method: http.MethodPost, path: "/cases/" + reviewReady + "/seal",
			body:  map[string]any{"reviewer": "Bram", "summary": "mixed batch seal"},
			apply: func(m *model) outcome { return m.sealPredict(reviewReady, "Bram", "mixed batch seal") }},
		{name: "reopen-sealed-terminal", method: http.MethodPost, path: "/cases/" + sealed + "/reopen",
			body:  map[string]any{"actor": "Lin", "reason": "cannot reopen sealed"},
			apply: func(m *model) outcome { return m.reopenPredict(sealed, "Lin", "cannot reopen sealed") }},
	}
	// 6! = 720 candidate serializations: bounded and exhaustive.
	d.runConcurrentBatch("cross-device-mixed", reqs)
}

// commitSealForSetup drives one seal through the oracle and the service during
// deterministic setup.
func (d *driver) commitSealForSetup(caseID, reviewer, summary string) {
	d.t.Helper()
	pred := d.m.sealPredict(caseID, reviewer, summary)
	res := d.mustDo(http.MethodPost, "/cases/"+caseID+"/seal", map[string]any{
		"reviewer": reviewer, "summary": summary,
	})
	if res.status != pred.status || res.code != pred.code {
		d.t.Fatalf("setup seal: got (%d,%s) want (%d,%s) body=%s",
			res.status, res.code, pred.status, pred.code, res.raw)
	}
}

// TestTerminalRetryMatrix enumerates retry verbs against every terminal or
// non-current state and proves each is rejected with the documented status and
// code while detail and event reads remain unchanged.
func TestTerminalRetryMatrix(t *testing.T) {
	h := newHarness(t)
	d := newDriver(t, h, 9007)

	_, awaiting := d.setupAwaiting("MET-M-AWAIT")
	_, recheck := d.setupAwaiting("MET-M-RECH")
	d.commitMeasurement(recheck, 26.4, "Rui")
	_, review := d.setupAwaiting("MET-M-REV")
	d.commitMeasurement(review, 25.2, "Rui")
	_, sealed := d.setupAwaiting("MET-M-SEAL")
	d.commitMeasurement(sealed, 25.2, "Rui")
	d.commitSealForSetup(sealed, "Anke", "matrix seal")

	cases := []struct {
		name       string
		caseID     string
		method     string
		path       string
		body       any
		wantStatus int
		wantCode   string
		mutates    bool // true when the call is the legal transition out of the state
	}{
		{"measure-on-review", review, http.MethodPost, "/cases/" + review + "/measurements",
			map[string]any{"observed": 25.0, "technician": "Rui"}, statusConflict, codeState, false},
		{"reopen-on-review", review, http.MethodPost, "/cases/" + review + "/reopen",
			map[string]any{"actor": "Lin", "reason": "wrong state"}, statusConflict, codeState, false},
		{"seal-on-already-sealed", sealed, http.MethodPost, "/cases/" + sealed + "/seal",
			map[string]any{"reviewer": "Bram", "summary": "second"}, statusConflict, codeSealExists, false},
		{"measure-on-sealed", sealed, http.MethodPost, "/cases/" + sealed + "/measurements",
			map[string]any{"observed": 25.0, "technician": "Rui"}, statusConflict, codeState, false},
		{"reopen-on-sealed", sealed, http.MethodPost, "/cases/" + sealed + "/reopen",
			map[string]any{"actor": "Lin", "reason": "terminal"}, statusConflict, codeState, false},
		{"reopen-on-awaiting", awaiting, http.MethodPost, "/cases/" + awaiting + "/reopen",
			map[string]any{"actor": "Lin", "reason": "never rechecked"}, statusConflict, codeState, false},
		{"seal-on-awaiting", awaiting, http.MethodPost, "/cases/" + awaiting + "/seal",
			map[string]any{"reviewer": "Anke", "summary": "no measurement"}, statusConflict, codeState, false},
		{"seal-on-recheck", recheck, http.MethodPost, "/cases/" + recheck + "/seal",
			map[string]any{"reviewer": "Anke", "summary": "rejected measurement"}, statusConflict, codeState, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := d.readDetail(tc.caseID)
			res, err := d.h.do(tc.method, tc.path, tc.body)
			if err != nil {
				t.Fatalf("%s transport: %v", tc.name, err)
			}
			if res.status != tc.wantStatus || res.code != tc.wantCode {
				t.Fatalf("%s: got (%d,%s), want (%d,%s); body=%s",
					tc.name, res.status, res.code, tc.wantStatus, tc.wantCode, res.raw)
			}
			if !tc.mutates {
				after := d.readDetail(tc.caseID)
				d.compareDetail(tc.name+" must leave every fact untouched", after, before)
			}
		})
	}

	// Terminal-state retry after the legal recheck -> awaiting transition: the
	// first reopen commits once; an immediate duplicate must be a clean
	// state_conflict and add no second event or version bump.
	t.Run("reopen-then-duplicate-retry", func(t *testing.T) {
		first, err := d.h.do(http.MethodPost, "/cases/"+recheck+"/reopen",
			map[string]any{"actor": "Lin", "reason": "first retry wins"})
		if err != nil {
			t.Fatal(err)
		}
		if first.status != http.StatusOK {
			t.Fatalf("legal reopen: status=%d body=%s", first.status, first.raw)
		}
		committed := d.readDetail(recheck)
		second, err := d.h.do(http.MethodPost, "/cases/"+recheck+"/reopen",
			map[string]any{"actor": "Mira", "reason": "duplicate must lose"})
		if err != nil {
			t.Fatal(err)
		}
		if second.status != http.StatusConflict || second.code != codeState {
			t.Fatalf("duplicate reopen: got (%d,%s), want (409,%s); body=%s",
				second.status, second.code, codeState, second.raw)
		}
		after := d.readDetail(recheck)
		d.compareDetail("duplicate reopen must leave every fact untouched", after, committed)
	})
}

// TestRollbackProbes deliberately drives the two service methods whose
// implementation mutates a cloned aggregate before validating the request
// body (reopen and seal). Each probe must be rolled back completely by the
// repository: no version increment, no event, no seal, no consumed id.
func TestRollbackProbes(t *testing.T) {
	t.Run("reopen-invalid-body-in-recheck-rolls-back", func(t *testing.T) {
		h := newHarness(t)
		d := newDriver(t, h, 9101)
		_, caseID := d.setupAwaiting("MET-RB-REOP")
		d.commitMeasurement(caseID, 26.4, "Rui") // recheck_required
		before := d.readDetail(caseID)

		res, err := d.h.do(http.MethodPost, "/cases/"+caseID+"/reopen",
			map[string]any{"actor": "   ", "reason": ""})
		if err != nil {
			t.Fatal(err)
		}
		if res.status != http.StatusBadRequest || res.code != codeInvalid {
			t.Fatalf("invalid reopen: got (%d,%s), want (400,%s); body=%s",
				res.status, res.code, codeInvalid, res.raw)
		}
		after := d.readDetail(caseID)
		d.compareDetail("invalid reopen must roll back case version and leave no event", after, before)
		if after.Case.State != stRecheck {
			t.Errorf("state after rolled-back reopen = %q, want recheck_required", after.Case.State)
		}
	})

	t.Run("seal-invalid-body-in-review-rolls-back", func(t *testing.T) {
		h := newHarness(t)
		d := newDriver(t, h, 9102)
		_, caseID := d.setupAwaiting("MET-RB-SEAL")
		d.commitMeasurement(caseID, 25.2, "Rui") // review_ready
		before := d.readDetail(caseID)

		res, err := d.h.do(http.MethodPost, "/cases/"+caseID+"/seal",
			map[string]any{"reviewer": "", "summary": "   "})
		if err != nil {
			t.Fatal(err)
		}
		if res.status != http.StatusBadRequest || res.code != codeInvalid {
			t.Fatalf("invalid seal: got (%d,%s), want (400,%s); body=%s",
				res.status, res.code, codeInvalid, res.raw)
		}
		after := d.readDetail(caseID)
		d.compareDetail("invalid seal must roll back sealed state, seal fact and event", after, before)
		if after.Case.State != stReview {
			t.Errorf("state after rolled-back seal = %q, want review_ready", after.Case.State)
		}
		if after.Seal != nil {
			t.Errorf("rollback left a seal fact behind: %+v", after.Seal)
		}
	})

	t.Run("seal-invalid-body-then-valid-seal-commits-once", func(t *testing.T) {
		h := newHarness(t)
		d := newDriver(t, h, 9103)
		_, caseID := d.setupAwaiting("MET-RB-RECOVER")
		d.commitMeasurement(caseID, 25.2, "Rui")

		bad, err := d.h.do(http.MethodPost, "/cases/"+caseID+"/seal",
			map[string]any{"reviewer": "", "summary": ""})
		if err != nil {
			t.Fatal(err)
		}
		if bad.status != http.StatusBadRequest || bad.code != codeInvalid {
			t.Fatalf("invalid seal: got (%d,%s), want 400/%s", bad.status, bad.code, codeInvalid)
		}

		// After a rolled-back attempt, a proper seal must still be the single
		// sealing commit; an id counter or clone leak would desync this view.
		d.commitSealForSetup(caseID, "Anke", "recovery seal")
		final := d.readDetail(caseID)
		if final.Case.State != stSealed || final.Seal == nil {
			t.Fatalf("case did not seal after rollback recovery: %+v", final.Case)
		}
		if final.Case.Version != 3 {
			t.Errorf("version = %d, want 3 (rolled-back attempt must not bump)", final.Case.Version)
		}
	})
}

// TestDeterminism proves fixed seeds reproduce identical observable decisions.
// The service stamps wall-clock times, so only the clock-independent facts
// (states, versions, accepted flags, event kinds, ids) are compared.
func TestDeterminism(t *testing.T) {
	capture := func(seed int64) []string {
		h := newHarness(t)
		d := newDriver(t, h, seed)
		d.checkCommitClock = false // this test compares clock-independent facts only
		d.runSequential(3, 120)
		var facts []string
		for _, id := range d.m.caseIDs() {
			detail := d.readDetail(id)
			facts = append(facts, summarizeDetail(detail)...)
		}
		return facts
	}
	first := capture(777)
	second := capture(777)
	if len(first) != len(second) {
		t.Fatalf("deterministic rerun produced %d facts then %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("fixed seed diverged at fact %d:\n  run1: %s\n  run2: %s", i, first[i], second[i])
		}
	}
}
