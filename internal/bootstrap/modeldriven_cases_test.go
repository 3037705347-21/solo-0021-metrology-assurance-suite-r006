package bootstrap_test

import (
	"bytes"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
)

var (
	modelSeeds = flag.String("model-seeds", "1,2,3,4,5", "comma-separated seeds for model-driven runs")
	modelSteps = flag.Int("model-steps", 220, "number of requests per model-driven run")
	raceRounds = flag.Int("race-rounds", 20, "rounds of each concurrent terminal/interleaving pattern")
)

func parseSeeds(t *testing.T) []int64 {
	t.Helper()
	var seeds []int64
	current := int64(0)
	seen := false
	for _, ch := range *modelSeeds {
		if ch == ',' {
			if seen {
				seeds = append(seeds, current)
			}
			current, seen = 0, false
			continue
		}
		if ch < '0' || ch > '9' {
			t.Fatalf("invalid --model-seeds value %q", *modelSeeds)
		}
		current = current*10 + int64(ch-'0')
		seen = true
	}
	if seen {
		seeds = append(seeds, current)
	}
	if len(seeds) == 0 {
		t.Fatalf("--model-seeds must contain at least one seed")
	}
	return seeds
}

// TestModelDrivenSequences is the main model-driven test. Each seed drives the
// real HTTP service through hundreds of mixed valid/invalid operations over
// several devices and cases. After every single request all detail and event
// reads must exactly match the specification model.
func TestModelDrivenSequences(t *testing.T) {
	for _, seed := range parseSeeds(t) {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			script := planScript(seed, *modelSteps)
			w, v := executeScript(t, script)
			if v == nil {
				t.Logf("seed %d: %d requests, %d devices, %d cases verified with no divergence",
					seed, *modelSteps, w.deviceCount(), len(w.caseOrder))
				return
			}

			// Report the phenomenon plus a minimal reproducer only; assertions
			// are never weakened to match observed behavior.
			reduced := shrinkScript(t, script, v)
			t.Errorf("MODEL DIVERGENCE\n%s\n--- minimal reproduction (%d of %d requests) ---\n%s\n--- reduced transcript ---\n%s",
				v.msg, len(reduced), len(script),
				renderExecutedScript(t, seed, reduced),
				renderExecutedScript(t, seed, reduced))
		})
	}
}

// TestSeedDeterminism proves the fixed seed produces a repeatable sequence, so
// a reported minimal reproduction can be replayed from a clean checkout.
func TestSeedDeterminism(t *testing.T) {
	const seed int64 = 777
	const count = 90
	first := planScript(seed, count)
	second := planScript(seed, count)
	if len(first) != len(second) {
		t.Fatalf("planned length differs: %d vs %d", len(first), len(second))
	}
	for i := range first {
		a, b := first[i], second[i]
		if a.kind != b.kind ||
			a.deviceSym != b.deviceSym || a.caseSym != b.caseSym ||
			a.newDeviceSym != b.newDeviceSym || a.newCaseSym != b.newCaseSym ||
			a.wantStatus != b.wantStatus || a.wantCode != b.wantCode ||
			a.tag != b.tag || a.rawID != b.rawID ||
			a.observed != b.observed || a.technician != b.technician ||
			a.actor != b.actor || a.reviewer != b.reviewer {
			t.Fatalf("seed is not deterministic at step %d:\n%s\n%s", i, mustJSON(a), mustJSON(b))
		}
	}

	// Execute the same seed twice against fresh servers: concrete id allocation
	// and responses must repeat as well.
	w1, v1 := executeScript(t, copyScript(first))
	if v1 != nil {
		t.Fatalf("first deterministic run failed: %s", v1.msg)
	}
	w2, v2 := executeScript(t, copyScript(second))
	if v2 != nil {
		t.Fatalf("second deterministic run failed: %s", v2.msg)
	}
	if len(w1.deviceOrder) != len(w2.deviceOrder) || len(w1.caseOrder) != len(w2.caseOrder) {
		t.Fatalf("deterministic runs created different resource counts")
	}
}

// TestTerminalStateRetries builds a case that reaches every terminal-ish state
// and replays each mutating route against it. Terminal-state retries must be
// rejected with state_conflict/seal_exists and leave no version bump, event,
// measurement, or seal behind.
func TestTerminalStateRetries(t *testing.T) {
	e := newEnv()
	defer e.close()
	h := newRaceHarness(t, e)

	// A sealed case on one device plus a recheck_required case on another.
	sealedCase, sealedDevice := h.readyCase("TERM-SEAL-1")
	h.seal(sealedCase, "Verifier-A", "seal the case once", http.StatusCreated, "")

	recheckCase, recheckDevice := h.rejectedCase("TERM-RECHECK-1")
	reviewCase, reviewDevice := h.readyCase("TERM-REVIEW-1")
	awaitingCase, awaitingDevice := h.openCaseForNewDevice("TERM-AWAIT-1")

	type probe struct {
		name   string
		method string
		path   string
		body   map[string]any
		code   string
	}
	probes := func(caseID string) []probe {
		return []probe{
			{"measure", http.MethodPost, "/cases/" + caseID + "/measurements",
				map[string]any{"observed": 25.1, "technician": "Rui"}, "state_conflict"},
			{"reopen", http.MethodPost, "/cases/" + caseID + "/reopen",
				map[string]any{"actor": "Lin", "reason": "retry"}, "state_conflict"},
		}
	}

	snapshotCase := func(path string) []byte {
		r := e.do(t, http.MethodGet, path, nil)
		if r.status != http.StatusOK {
			t.Fatalf("snapshot GET %s: %d %s", path, r.status, r.raw)
		}
		return r.raw
	}

	sealedProbes := append(probes(sealedCase), probe{
		"seal", http.MethodPost, "/cases/" + sealedCase + "/seal",
		map[string]any{"reviewer": "Verifier-B", "summary": "retry seal"}, "seal_exists",
	})

	// review_ready: measure/reopen are state conflicts, seal is a terminal
	// transition; the retries before sealing must leave no trace.
	for _, p := range probes(reviewCase) {
		casePath := "/cases/" + reviewCase
		before := snapshotCase(casePath)
		r := e.do(t, http.MethodPost, p.path, p.body)
		assertStatusCode(t, r, http.StatusConflict, p.code, p.name)
		assertUnchangedRead(t, e, casePath, before)
	}

	// awaiting: reopen/seal are state conflicts; an invalid measurement is a
	// validation rejection. A valid measurement is a legal transition and is
	// tested elsewhere, never as a retry.
	awaitingPath := "/cases/" + awaitingCase
	before := snapshotCase(awaitingPath)
	r := e.do(t, http.MethodPost, awaitingPath+"/reopen",
		map[string]any{"actor": "Lin", "reason": "retry"})
	assertStatusCode(t, r, http.StatusConflict, "state_conflict", "reopen awaiting")
	assertUnchangedRead(t, e, awaitingPath, before)
	r = e.do(t, http.MethodPost, awaitingPath+"/seal",
		map[string]any{"reviewer": "V", "summary": "s"})
	assertStatusCode(t, r, http.StatusConflict, "state_conflict", "seal awaiting")
	assertUnchangedRead(t, e, awaitingPath, before)
	r = e.do(t, http.MethodPost, awaitingPath+"/measurements",
		map[string]any{"observed": 25.1, "technician": "   "})
	assertStatusCode(t, r, http.StatusBadRequest, "invalid_input", "measure awaiting blank technician")
	assertUnchangedRead(t, e, awaitingPath, before)

	// recheck_required: seal is conflict; reopen succeeds once, after which a
	// second reopen and measurements are conflicts.
	r = e.do(t, http.MethodPost, "/cases/"+recheckCase+"/seal",
		map[string]any{"reviewer": "V", "summary": "s"})
	assertStatusCode(t, r, http.StatusConflict, "state_conflict", "seal recheck")
	r = e.do(t, http.MethodPost, "/cases/"+recheckCase+"/measurements",
		map[string]any{"observed": 25.1, "technician": "Rui"})
	assertStatusCode(t, r, http.StatusConflict, "state_conflict", "measure recheck")

	for _, p := range sealedProbes {
		casePath := "/cases/" + sealedCase
		before := snapshotCase(casePath)
		r := e.do(t, http.MethodPost, p.path, p.body)
		assertStatusCode(t, r, http.StatusConflict, p.code, "sealed:"+p.name)
		assertUnchangedRead(t, e, casePath, before)
	}

	// Device must still be unable to open a second unfinished case (sealed
	// allows a new one; active unfinished cases block it).
	r = e.do(t, http.MethodPost, "/cases", map[string]any{
		"device_id": recheckDevice, "nominal": 25.0, "tolerance": 0.5, "steward": "Q",
	})
	assertStatusCode(t, r, http.StatusConflict, "active_case_exists", "open while unfinished recheck")
	r = e.do(t, http.MethodPost, "/cases", map[string]any{
		"device_id": reviewDevice, "nominal": 25.0, "tolerance": 0.5, "steward": "Q",
	})
	assertStatusCode(t, r, http.StatusConflict, "active_case_exists", "open while unfinished review")
	r = e.do(t, http.MethodPost, "/cases", map[string]any{
		"device_id": awaitingDevice, "nominal": 25.0, "tolerance": 0.5, "steward": "Q",
	})
	assertStatusCode(t, r, http.StatusConflict, "active_case_exists", "open while unfinished awaiting")

	// The sealed device can open exactly one follow-up case; repeating it is
	// rejected, and the earlier sealed case stays untouched.
	opened := e.do(t, http.MethodPost, "/cases", map[string]any{
		"device_id": sealedDevice, "nominal": 10.0, "tolerance": 0.2, "steward": "Q",
	})
	assertStatus(t, opened, http.StatusCreated)
	second := e.do(t, http.MethodPost, "/cases", map[string]any{
		"device_id": sealedDevice, "nominal": 10.0, "tolerance": 0.2, "steward": "Q",
	})
	assertStatusCode(t, second, http.StatusConflict, "active_case_exists", "second follow-up open")
	view := readCase(t, e, sealedCase)
	if string(view.Case.State) != stateSealed || view.Seal == nil {
		t.Fatalf("original sealed case changed after follow-up open: %s", mustJSON(view))
	}
}

// TestRepositoryCallbackFailures exercises the copy-on-write rollback path via
// failures that occur *inside* the repository callback (after the state machine
// has already mutated its working copy): seal field validation, reopen
// actor/reason validation, and measurement technician validation. None of
// these may leave a version, measurement, event, or seal, and none may consume
// an externally visible id sequence number.
func TestRepositoryCallbackFailures(t *testing.T) {
	t.Run("seal validation rollback keeps review-ready state", func(t *testing.T) {
		e := newEnv()
		defer e.close()
		h := newRaceHarness(t, e)
		caseID, _ := h.readyCase("ROLLBACK-SEAL-1")

		before := readCase(t, e, caseID)
		beforeEvents := e.do(t, http.MethodGet, "/cases/"+caseID+"/events", nil).raw

		for _, body := range []map[string]any{
			{"reviewer": "", "summary": "summary present"},
			{"reviewer": "Verifier", "summary": "   "},
			{"reviewer": " ", "summary": ""},
		} {
			r := e.do(t, http.MethodPost, "/cases/"+caseID+"/seal", body)
			assertStatusCode(t, r, http.StatusBadRequest, "invalid_input", "seal missing field")
			assertUnchangedRead(t, e, "/cases/"+caseID, mustJSON(before))
			assertUnchangedRead(t, e, "/cases/"+caseID+"/events", beforeEvents)
		}

		// A subsequent valid seal must use the *first* seal id: rolled-back
		// transactions consumed neither the seal id nor an event id.
		r := e.do(t, http.MethodPost, "/cases/"+caseID+"/seal",
			map[string]any{"reviewer": "Verifier", "summary": "now valid"})
		assertStatus(t, r, http.StatusCreated)
		after := readCase(t, e, caseID)
		if after.Seal == nil || after.Seal.ID != "seal-0001" {
			t.Fatalf("rollback consumed seal sequence: got seal %v", after.Seal)
		}
		if len(after.Events) != len(before.Events)+1 {
			t.Fatalf("expected exactly one new event after rollbacks, before=%d after=%d",
				len(before.Events), len(after.Events))
		}
		want := nextSeqID(before.Events[len(before.Events)-1].ID)
		if got := after.Events[len(after.Events)-1].ID; got != want {
			t.Fatalf("sealed event id=%q, want %q (rollback appears to have consumed event ids)", got, want)
		}

		// Repeat seal after the committed one is seal_exists and changes nothing.
		again := e.do(t, http.MethodPost, "/cases/"+caseID+"/seal",
			map[string]any{"reviewer": "Other", "summary": "again"})
		assertStatusCode(t, again, http.StatusConflict, "seal_exists", "seal retry")
		assertUnchangedRead(t, e, "/cases/"+caseID, mustJSON(after))
	})

	t.Run("reopen validation rollback keeps recheck state", func(t *testing.T) {
		e := newEnv()
		defer e.close()
		h := newRaceHarness(t, e)
		caseID, _ := h.rejectedCase("ROLLBACK-REOPEN-1")
		before := readCase(t, e, caseID)

		r := e.do(t, http.MethodPost, "/cases/"+caseID+"/reopen",
			map[string]any{"actor": "", "reason": "setup replaced"})
		assertStatusCode(t, r, http.StatusBadRequest, "invalid_input", "reopen missing actor")
		assertUnchangedRead(t, e, "/cases/"+caseID, mustJSON(before))

		r = e.do(t, http.MethodPost, "/cases/"+caseID+"/reopen",
			map[string]any{"actor": "Lin", "reason": " "})
		assertStatusCode(t, r, http.StatusBadRequest, "invalid_input", "reopen blank reason")
		assertUnchangedRead(t, e, "/cases/"+caseID, mustJSON(before))

		r = e.do(t, http.MethodPost, "/cases/"+caseID+"/seal",
			map[string]any{"reviewer": "V", "summary": "s"})
		assertStatusCode(t, r, http.StatusConflict, "state_conflict", "seal after rolled-back reopen")
		r = e.do(t, http.MethodPost, "/cases/"+caseID+"/measurements",
			map[string]any{"observed": 25.1, "technician": "Rui"})
		assertStatusCode(t, r, http.StatusConflict, "state_conflict", "measure after rolled-back reopen")

		// Valid reopen still works and consumes exactly the next event id.
		r = e.do(t, http.MethodPost, "/cases/"+caseID+"/reopen",
			map[string]any{"actor": "Lin", "reason": "setup replaced"})
		assertStatus(t, r, http.StatusOK)
		after := readCase(t, e, caseID)
		if string(after.Case.State) != stateAwaiting || after.Case.Version != before.Case.Version+1 {
			t.Fatalf("reopen state wrong after rollback: state=%s version=%d", after.Case.State, after.Case.Version)
		}
		want := nextSeqID(before.Events[len(before.Events)-1].ID)
		if got := after.Events[len(after.Events)-1].ID; got != want {
			t.Fatalf("reopen event id=%q want %q", got, want)
		}
	})

	t.Run("measurement validation rollback keeps awaiting state", func(t *testing.T) {
		e := newEnv()
		defer e.close()
		h := newRaceHarness(t, e)
		caseID, _ := h.openCaseForNewDevice("ROLLBACK-MEASURE-1")
		before := readCase(t, e, caseID)

		r := e.do(t, http.MethodPost, "/cases/"+caseID+"/measurements",
			map[string]any{"observed": 25.2, "technician": "   "})
		assertStatusCode(t, r, http.StatusBadRequest, "invalid_input", "measure blank technician")
		assertUnchangedRead(t, e, "/cases/"+caseID, mustJSON(before))

		// The following valid measurement gets measurement-0001 and event ids
		// continue without gaps.
		r = e.do(t, http.MethodPost, "/cases/"+caseID+"/measurements",
			map[string]any{"observed": 25.2, "technician": "Rui"})
		assertStatus(t, r, http.StatusCreated)
		after := readCase(t, e, caseID)
		if len(after.Measurements) != 1 || after.Measurements[0].ID != "measurement-0001" {
			t.Fatalf("rollback consumed measurement sequence: %v", after.Measurements)
		}
		want := nextSeqID(before.Events[len(before.Events)-1].ID)
		if got := after.Events[len(after.Events)-1].ID; got != want {
			t.Fatalf("measurement event id=%q want %q", got, want)
		}
	})

	t.Run("failed registration does not reserve tag or ids", func(t *testing.T) {
		e := newEnv()
		defer e.close()

		r := e.do(t, http.MethodPost, "/devices", map[string]any{
			"asset_tag": "ROLLBACK-TAG-1", "model": "", "room": "Room A", "steward": "Lin",
		})
		assertStatusCode(t, r, http.StatusBadRequest, "invalid_input", "invalid device")

		r = e.do(t, http.MethodPost, "/devices", map[string]any{
			"asset_tag": "rollback-tag-1", "model": "Bench", "room": "Room A", "steward": "Lin",
		})
		assertStatus(t, r, http.StatusCreated)
		if id := extractDeviceID(t, r); id != "device-0001" {
			t.Fatalf("rolled-back registration consumed an id: got %q want device-0001", id)
		}
	})

	t.Run("malformed bodies never touch state", func(t *testing.T) {
		e := newEnv()
		defer e.close()
		h := newRaceHarness(t, e)
		caseID, _ := h.readyCase("ROLLBACK-BODY-1")
		recheckID, _ := h.rejectedCase("ROLLBACK-BODY-2")
		before := e.do(t, http.MethodGet, "/cases/"+caseID, nil)
		recheckBefore := e.do(t, http.MethodGet, "/cases/"+recheckID, nil)

		type badRequest struct {
			caseID     string
			beforeRaw  []byte
			path       string
			raw        string
			wantStatus int
			wantCode   string
		}
		requests := []badRequest{
			{caseID, before.raw, "/devices", `{"asset_tag":`, http.StatusBadRequest, "invalid_input"},
			{caseID, before.raw, "/devices", `{"asset_tag":"X","model":"m","room":"r","steward":"s","extra":1}`, http.StatusBadRequest, "invalid_input"},
			{caseID, before.raw, "/cases", `{"device_id":"`, http.StatusBadRequest, "invalid_input"},
			{caseID, before.raw, "/cases/" + caseID + "/measurements", `{not json`, http.StatusBadRequest, "invalid_input"},
			{caseID, before.raw, "/cases/" + caseID + "/measurements", `{"observed":1,"technician":"R","extra":1}`, http.StatusBadRequest, "invalid_input"},
			// review_ready rejects reopen on state before validating fields.
			{caseID, before.raw, "/cases/" + caseID + "/reopen", `{"actor":"Lin"}`, http.StatusConflict, "state_conflict"},
			// review_ready passes the state gate; missing summary is a callback validation failure.
			{caseID, before.raw, "/cases/" + caseID + "/seal", `{"reviewer":"Lin"}`, http.StatusBadRequest, "invalid_input"},
			// recheck_required passes the state gate; missing reason is a callback validation failure.
			{recheckID, recheckBefore.raw, "/cases/" + recheckID + "/reopen", `{"actor":"Lin"}`, http.StatusBadRequest, "invalid_input"},
		}
		for _, req := range requests {
			result := e.doRaw(t, http.MethodPost, req.path, bytes.NewBufferString(req.raw), true)
			if result.status != req.wantStatus || errorCode(result.raw) != req.wantCode {
				t.Fatalf("body %s -> %d %s, want %d %s", req.raw, result.status, result.raw, req.wantStatus, req.wantCode)
			}
			after := e.do(t, http.MethodGet, "/cases/"+req.caseID, nil)
			if !bytes.Equal(req.beforeRaw, after.raw) {
				t.Fatalf("malformed request changed case %s state:\nbefore=%s\nafter =%s", req.caseID, req.beforeRaw, after.raw)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Interleaved / concurrent request patterns
// ---------------------------------------------------------------------------

type raceHarness struct {
	t *testing.T
	e *env
}

func newRaceHarness(t *testing.T, e *env) *raceHarness { return &raceHarness{t: t, e: e} }

func (h *raceHarness) createDevice(tag string) string {
	r := h.e.do(h.t, http.MethodPost, "/devices", map[string]any{
		"asset_tag": tag, "model": "Bench", "room": "Room A", "steward": "Lin",
	})
	assertStatus(h.t, r, http.StatusCreated)
	return extractDeviceID(h.t, r)
}

func (h *raceHarness) openCaseForNewDevice(tag string) (caseID, deviceID string) {
	deviceID = h.createDevice(tag)
	r := h.e.do(h.t, http.MethodPost, "/cases", map[string]any{
		"device_id": deviceID, "nominal": 25.0, "tolerance": 0.5, "steward": "Qiao",
	})
	assertStatus(h.t, r, http.StatusCreated)
	caseID = extractCaseID(h.t, r)
	return caseID, deviceID
}

func (h *raceHarness) rejectedCase(tag string) (caseID, deviceID string) {
	caseID, deviceID = h.openCaseForNewDevice(tag)
	r := h.e.do(h.t, http.MethodPost, "/cases/"+caseID+"/measurements",
		map[string]any{"observed": 26.5, "technician": "Rui"})
	assertStatus(h.t, r, http.StatusCreated)
	return caseID, deviceID
}

func (h *raceHarness) readyCase(tag string) (caseID, deviceID string) {
	caseID, deviceID = h.openCaseForNewDevice(tag)
	r := h.e.do(h.t, http.MethodPost, "/cases/"+caseID+"/measurements",
		map[string]any{"observed": 25.2, "technician": "Rui"})
	assertStatus(h.t, r, http.StatusCreated)
	return caseID, deviceID
}

func (h *raceHarness) seal(caseID, reviewer, summary string, wantStatus int, wantCode string) httpResult {
	r := h.e.do(h.t, http.MethodPost, "/cases/"+caseID+"/seal",
		map[string]any{"reviewer": reviewer, "summary": summary})
	assertStatusCode(h.t, r, wantStatus, wantCode, "seal")
	return r
}

type racedRequest struct {
	name   string
	method string
	path   string
	body   map[string]any
}

type racedResult struct {
	name   string
	result httpResult
}

// runRaced fires all requests concurrently against a barrier so they overlap
// inside the repository critical section window.
func (h *raceHarness) runRaced(requests []racedRequest) []racedResult {
	var wg sync.WaitGroup
	results := make([]racedResult, len(requests))
	start := make(chan struct{})
	for i, req := range requests {
		wg.Add(1)
		go func(i int, req racedRequest) {
			defer wg.Done()
			<-start
			results[i] = racedResult{name: req.name, result: h.e.do(h.t, req.method, req.path, req.body)}
		}(i, req)
	}
	close(start)
	wg.Wait()
	return results
}

func TestConcurrentSealRaces(t *testing.T) {
	for round := 0; round < *raceRounds; round++ {
		t.Run(fmt.Sprintf("round-%02d", round), func(t *testing.T) {
			e := newEnv()
			defer e.close()
			h := newRaceHarness(t, e)
			caseID, _ := h.readyCase(fmt.Sprintf("RACE-SEAL-%02d", round))

			reqs := []racedRequest{
				{"a", http.MethodPost, "/cases/" + caseID + "/seal",
					map[string]any{"reviewer": "Reviewer-A", "summary": "seal race A"}},
				{"b", http.MethodPost, "/cases/" + caseID + "/seal",
					map[string]any{"reviewer": "Reviewer-B", "summary": "seal race B"}},
				{"c", http.MethodPost, "/cases/" + caseID + "/seal",
					map[string]any{"reviewer": "Reviewer-C", "summary": "seal race C"}},
			}
			outcomes := h.runRaced(reqs)

			created, conflicts := 0, 0
			for _, out := range outcomes {
				switch out.result.status {
				case http.StatusCreated:
					created++
				case http.StatusConflict:
					if errorCode(out.result.raw) != "seal_exists" {
						t.Fatalf("losing seal race returned code %q: %s", errorCode(out.result.raw), out.result.raw)
					}
					conflicts++
				default:
					t.Fatalf("seal race status=%d body=%s", out.result.status, out.result.raw)
				}
			}
			if created != 1 || conflicts != len(reqs)-1 {
				t.Fatalf("seal race outcomes: %d created, %d conflicts", created, conflicts)
			}

			view := readCase(t, e, caseID)
			if string(view.Case.State) != stateSealed || view.Seal == nil {
				t.Fatalf("seal race left case not sealed: %s", mustJSON(view))
			}
			if view.Seal.ID != "seal-0001" {
				t.Fatalf("seal race produced unexpected seal id %q", view.Seal.ID)
			}
			kindCounts := map[string]int{}
			for _, ev := range view.Events {
				kindCounts[ev.Kind]++
			}
			if kindCounts["case_sealed"] != 1 {
				t.Fatalf("seal race appended %d case_sealed events", kindCounts["case_sealed"])
			}
			if view.Case.Version != 3 { // open=1, accepted measurement=2, seal=3
				t.Fatalf("seal race version=%d want 3", view.Case.Version)
			}
		})
	}
}

func TestConcurrentMeasurementRaces(t *testing.T) {
	t.Run("accepted measurements serialize", func(t *testing.T) {
		for round := 0; round < *raceRounds; round++ {
			e := newEnv()
			defer e.close()
			h := newRaceHarness(t, e)
			caseID, _ := h.openCaseForNewDevice(fmt.Sprintf("RACE-MEAS-OK-%02d", round))

			reqs := []racedRequest{
				{"a", http.MethodPost, "/cases/" + caseID + "/measurements",
					map[string]any{"observed": 25.1, "technician": "Tech-A"}},
				{"b", http.MethodPost, "/cases/" + caseID + "/measurements",
					map[string]any{"observed": 25.2, "technician": "Tech-B"}},
			}
			outcomes := h.runRaced(reqs)

			created, conflicts := 0, 0
			for _, out := range outcomes {
				switch out.result.status {
				case http.StatusCreated:
					created++
				case http.StatusConflict:
					if errorCode(out.result.raw) != "state_conflict" {
						t.Fatalf("measure race code=%q", errorCode(out.result.raw))
					}
					conflicts++
				default:
					t.Fatalf("measure race status=%d body=%s", out.result.status, out.result.raw)
				}
			}
			if created != 1 || conflicts != 1 {
				t.Fatalf("accepted measurement race: %d created %d conflicts", created, conflicts)
			}
			view := readCase(t, e, caseID)
			if string(view.Case.State) != stateReview || len(view.Measurements) != 1 {
				t.Fatalf("accepted measurement race left: %s", mustJSON(view))
			}
			if len(view.Events) != 2 || view.Events[1].Kind != "measurement_accepted" {
				t.Fatalf("accepted measurement race events wrong: %s", mustJSON(view.Events))
			}
			if view.Case.Version != 2 {
				t.Fatalf("version=%d want 2", view.Case.Version)
			}
		}
	})

	t.Run("rejected measurements serialize", func(t *testing.T) {
		for round := 0; round < *raceRounds; round++ {
			e := newEnv()
			defer e.close()
			h := newRaceHarness(t, e)
			caseID, _ := h.openCaseForNewDevice(fmt.Sprintf("RACE-MEAS-BAD-%02d", round))

			reqs := []racedRequest{
				{"a", http.MethodPost, "/cases/" + caseID + "/measurements",
					map[string]any{"observed": 26.4, "technician": "Tech-A"}},
				{"b", http.MethodPost, "/cases/" + caseID + "/measurements",
					map[string]any{"observed": 26.6, "technician": "Tech-B"}},
			}
			outcomes := h.runRaced(reqs)

			created, conflicts := 0, 0
			for _, out := range outcomes {
				switch out.result.status {
				case http.StatusCreated:
					created++
				case http.StatusConflict:
					conflicts++
				default:
					t.Fatalf("status=%d body=%s", out.result.status, out.result.raw)
				}
			}
			if created != 1 || conflicts != 1 {
				t.Fatalf("rejected measurement race: %d created %d conflicts", created, conflicts)
			}
			view := readCase(t, e, caseID)
			if string(view.Case.State) != stateRecheck || len(view.Measurements) != 1 {
				t.Fatalf("rejected measurement race left: %s", mustJSON(view))
			}
			if view.Events[1].Kind != "measurement_rejected" {
				t.Fatalf("event kind=%q want measurement_rejected", view.Events[1].Kind)
			}
		}
	})
}

func TestConcurrentReopenRaces(t *testing.T) {
	for round := 0; round < *raceRounds; round++ {
		e := newEnv()
		defer e.close()
		h := newRaceHarness(t, e)
		caseID, _ := h.rejectedCase(fmt.Sprintf("RACE-REOPEN-%02d", round))

		reqs := []racedRequest{
			{"a", http.MethodPost, "/cases/" + caseID + "/reopen",
				map[string]any{"actor": "Steward-A", "reason": "reopen race A"}},
			{"b", http.MethodPost, "/cases/" + caseID + "/reopen",
				map[string]any{"actor": "Steward-B", "reason": "reopen race B"}},
		}
		outcomes := h.runRaced(reqs)

		ok, conflicts := 0, 0
		for _, out := range outcomes {
			switch out.result.status {
			case http.StatusOK:
				ok++
			case http.StatusConflict:
				if errorCode(out.result.raw) != "state_conflict" {
					t.Fatalf("reopen race code=%q", errorCode(out.result.raw))
				}
				conflicts++
			default:
				t.Fatalf("reopen race status=%d body=%s", out.result.status, out.result.raw)
			}
		}
		if ok != 1 || conflicts != 1 {
			t.Fatalf("reopen race: %d ok %d conflicts", ok, conflicts)
		}
		view := readCase(t, e, caseID)
		if string(view.Case.State) != stateAwaiting {
			t.Fatalf("reopen race state=%s", view.Case.State)
		}
		count := 0
		for _, ev := range view.Events {
			if ev.Kind == "case_reopened" {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("reopen race appended %d case_reopened events", count)
		}
	}
}

// TestConcurrentCrossTransitionRace fires reopen + measurement concurrently
// from recheck_required. Exactly one transition must commit; the two linearized
// outcomes are both legal, but a half-state (state advanced without event) is
// not.
func TestConcurrentCrossTransitionRace(t *testing.T) {
	for round := 0; round < *raceRounds; round++ {
		e := newEnv()
		defer e.close()
		h := newRaceHarness(t, e)
		caseID, _ := h.rejectedCase(fmt.Sprintf("RACE-CROSS-%02d", round))

		reqs := []racedRequest{
			{"reopen", http.MethodPost, "/cases/" + caseID + "/reopen",
				map[string]any{"actor": "Steward-A", "reason": "cross race reopen"}},
			{"measure", http.MethodPost, "/cases/" + caseID + "/measurements",
				map[string]any{"observed": 25.2, "technician": "Tech-B"}},
		}
		outcomes := h.runRaced(reqs)

		reopenOK, measureOK := false, false
		for _, out := range outcomes {
			switch out.name {
			case "reopen":
				switch out.result.status {
				case http.StatusOK:
					reopenOK = true
				case http.StatusConflict:
				default:
					t.Fatalf("cross race reopen status=%d body=%s", out.result.status, out.result.raw)
				}
			case "measure":
				switch out.result.status {
				case http.StatusCreated:
					measureOK = true
				case http.StatusConflict:
				default:
					t.Fatalf("cross race measure status=%d body=%s", out.result.status, out.result.raw)
				}
			}
		}
		if !reopenOK {
			t.Fatalf("cross race: the valid reopen did not commit (reopen=%v measure=%v)", reopenOK, measureOK)
		}

		view := readCase(t, e, caseID)
		kinds := make([]string, 0, len(view.Events))
		for _, ev := range view.Events {
			kinds = append(kinds, ev.Kind)
		}
		switch {
		case !measureOK:
			// Reopen serialized first; the measurement was rejected against
			// recheck_required and must not appear at all.
			if string(view.Case.State) != stateAwaiting || len(view.Measurements) != 1 {
				t.Fatalf("reopen-only linearization wrong: %s", mustJSON(view))
			}
			if kinds[len(kinds)-1] != "case_reopened" || view.Case.Version != 3 {
				t.Fatalf("reopen-only linearization events/version wrong: kinds=%v version=%d", kinds, view.Case.Version)
			}
		default:
			// Both committed; the only legal order is reopen then measurement
			// (a measurement could not commit first and let reopen succeed).
			if string(view.Case.State) != stateReview || len(view.Measurements) != 2 {
				t.Fatalf("both-commit linearization wrong: %s", mustJSON(view))
			}
			want := []string{"case_opened", "measurement_rejected", "case_reopened", "measurement_accepted"}
			if len(kinds) != len(want) {
				t.Fatalf("both-commit events wrong: %v", kinds)
			}
			for i := range want {
				if kinds[i] != want[i] {
					t.Fatalf("both-commit event order wrong: got %v want %v", kinds, want)
				}
			}
			if view.Case.Version != 4 {
				t.Fatalf("both-commit version=%d want 4", view.Case.Version)
			}
		}
	}
}

// TestConcurrentDuplicateRegistration races identical normalized tags: exactly
// one device may be created.
func TestConcurrentDuplicateRegistration(t *testing.T) {
	for round := 0; round < *raceRounds; round++ {
		e := newEnv()
		defer e.close()
		h := newRaceHarness(t, e)
		tag := fmt.Sprintf("RACE-DUP-%04d", round)

		reqs := []racedRequest{
			{"a", http.MethodPost, "/devices",
				map[string]any{"asset_tag": " " + strings.ToLower(tag) + " ", "model": "A", "room": "R", "steward": "S"}},
			{"b", http.MethodPost, "/devices",
				map[string]any{"asset_tag": tag, "model": "B", "room": "R", "steward": "T"}},
		}
		outcomes := h.runRaced(reqs)

		created, conflicts := 0, 0
		for _, out := range outcomes {
			switch out.result.status {
			case http.StatusCreated:
				created++
			case http.StatusConflict:
				if errorCode(out.result.raw) != "duplicate_asset_tag" {
					t.Fatalf("dup race code=%q", errorCode(out.result.raw))
				}
				conflicts++
			default:
				t.Fatalf("dup race status=%d body=%s", out.result.status, out.result.raw)
			}
		}
		if created != 1 || conflicts != 1 {
			t.Fatalf("duplicate registration race: %d created %d conflicts", created, conflicts)
		}
	}
}

// TestConcurrentOpenCaseRaces races two case openings for one active device:
// exactly one may commit.
func TestConcurrentOpenCaseRaces(t *testing.T) {
	for round := 0; round < *raceRounds; round++ {
		e := newEnv()
		defer e.close()
		h := newRaceHarness(t, e)
		deviceID := h.createDevice(fmt.Sprintf("RACE-OPEN-%04d", round))

		reqs := []racedRequest{
			{"a", http.MethodPost, "/cases",
				map[string]any{"device_id": deviceID, "nominal": 25.0, "tolerance": 0.5, "steward": "Q"}},
			{"b", http.MethodPost, "/cases",
				map[string]any{"device_id": deviceID, "nominal": 10.0, "tolerance": 0.1, "steward": "L"}},
		}
		outcomes := h.runRaced(reqs)

		created, conflicts := 0, 0
		for _, out := range outcomes {
			switch out.result.status {
			case http.StatusCreated:
				created++
			case http.StatusConflict:
				if errorCode(out.result.raw) != "active_case_exists" {
					t.Fatalf("open race code=%q", errorCode(out.result.raw))
				}
				conflicts++
			default:
				t.Fatalf("open race status=%d body=%s", out.result.status, out.result.raw)
			}
		}
		if created != 1 || conflicts != 1 {
			t.Fatalf("open race: %d created %d conflicts", created, conflicts)
		}
	}
}
