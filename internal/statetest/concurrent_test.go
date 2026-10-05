package statetest_test

import (
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"time"
)

var zeroTime = time.Time{}

func deepEqualDetail(a, b caseDetail) bool { return reflect.DeepEqual(a, b) }
func deepEqualDevice(a, b viewDevice) bool { return reflect.DeepEqual(a, b) }

// concRequest is one request fired inside a concurrent batch. apply advances a
// candidate model copy along one possible serialization and predicts the
// outcome that serial order implies.
type concRequest struct {
	name    string
	method  string
	path    string
	body    any
	raw     []byte
	apply   func(m *model) outcome
	wantTag string // short label used in failure reports
}

// concObserved records what the real server returned for one batch request.
type concObserved struct {
	name   string
	result rpcResult
	err    error
}

// runConcurrentBatch fires every request from goroutines released by one
// channel barrier so they genuinely race on the repository mutex, then proves
// the aggregate behavior is explainable by at least one total serialization
// that (a) predicts every per-request status/code and (b) ends in exactly the
// state the live service reports. If no serialization exists, the interleaving
// exposed a non-atomic commit or a lost decision.
func (d *driver) runConcurrentBatch(label string, reqs []concRequest) {
	d.t.Helper()

	start := make(chan struct{})
	var wg sync.WaitGroup
	observed := make([]concObserved, len(reqs))
	for i, req := range reqs {
		wg.Add(1)
		go func(i int, req concRequest) {
			defer wg.Done()
			<-start
			res, err := d.h.do(req.method, req.path, req.body, req.raw)
			observed[i] = concObserved{name: req.name, result: res, err: err}
		}(i, req)
	}
	close(start)
	wg.Wait()

	for _, obs := range observed {
		if obs.err != nil {
			d.t.Fatalf("%s: request %s transport error: %v", label, obs.name, obs.err)
		}
	}

	winner, explained := d.explainableBySerialization(reqs, observed)
	if !explained {
		d.t.Errorf("%s: concurrent outcomes are not explainable by any legal serialization:", label)
		for _, obs := range observed {
			d.t.Errorf("  %-22s -> status=%d code=%s body=%s", obs.name, obs.result.status, obs.result.code, obs.result.raw)
		}
		d.t.Errorf("  the batch left a state no sequential execution of the same requests can produce")
		return
	}
	// Adopt the winning serialization as the oracle's new world: global
	// counters (and therefore the next event/measurement/seal ids) depend on
	// commit order, so only the candidate that matches live state can predict
	// later reads.
	d.m = winner

	// Regardless of which serialization won, the whole world must satisfy the
	// structural and state-machine invariants on every case and device.
	d.reconcileEverything(label + " post-concurrent reconciliation")

	// Retries of terminal mutations after the batch settles must all be clean
	// conflicts with zero additional facts.
	d.assertTerminalRejectionsAfterSettled()
}

// explainableBySerialization enumerates every permutation as a candidate
// serialization. A candidate is admissible when each oracle-predicted outcome
// equals the observed status/code and no candidate step produced an internal
// error. At least one admissible candidate must finish in the live state.
func (d *driver) explainableBySerialization(reqs []concRequest, observed []concObserved) (*model, bool) {
	obsByName := map[string]concObserved{}
	for _, obs := range observed {
		obsByName[obs.name] = obs
	}

	n := len(reqs)
	index := make([]int, n)
	for i := range index {
		index[i] = i
	}
	var permutations [][]int
	permute(index, 0, &permutations)
	for _, order := range permutations {
		candidate := d.m.deepCopy()
		admissible := true
		for _, idx := range order {
			req := reqs[idx]
			pred := req.apply(candidate)
			obs := obsByName[req.name]
			if pred.status != obs.result.status || pred.code != obs.result.code {
				admissible = false
				break
			}
		}
		if !admissible {
			continue
		}
		if d.matchesLiveState(candidate) {
			return candidate, true
		}
	}
	return nil, false
}

// matchesLiveState reads every case/device the candidate model knows and
// compares it structurally to the service.
func (d *driver) matchesLiveState(candidate *model) bool {
	for _, id := range candidate.caseIDs() {
		res, err := d.h.do(http.MethodGet, "/cases/"+id, nil)
		if err != nil || res.status != http.StatusOK || res.detail == nil {
			return false
		}
		gn := normalizeTimes(*res.detail)
		wn := normalizeTimes(candidate.cases[id].detail)
		if !deepEqualDetail(gn, wn) {
			return false
		}
	}
	for _, id := range candidate.deviceIDs() {
		res, err := d.h.do(http.MethodGet, "/devices/"+id, nil)
		if err != nil || res.status != http.StatusOK || res.device == nil {
			return false
		}
		got := *res.device
		got.EnrolledAt = zeroTime
		want := candidate.devices[id].device
		want.EnrolledAt = zeroTime
		if !deepEqualDevice(got, want) {
			return false
		}
	}
	return true
}

func permute(items []int, offset int, sink *[][]int) {
	if offset == len(items)-1 {
		cp := append([]int(nil), items...)
		*sink = append(*sink, cp)
		return
	}
	for i := offset; i < len(items); i++ {
		items[offset], items[i] = items[i], items[offset]
		permute(items, offset+1, sink)
		items[offset], items[i] = items[i], items[offset]
	}
}

// assertTerminalRejectionsAfterSettled replays mutating verbs against every
// sealed case. All must be rejected and none may alter version/events/seal.
func (d *driver) assertTerminalRejectionsAfterSettled() {
	d.t.Helper()
	for _, id := range d.m.caseIDs() {
		c := d.m.cases[id].detail
		if c.Case.State != stSealed {
			continue
		}
		before := d.readDetail(id)
		probes := []struct {
			name   string
			method string
			path   string
			body   any
		}{
			{"measure", http.MethodPost, "/cases/" + id + "/measurements", map[string]any{"observed": c.Case.Nominal, "technician": "Rui"}},
			{"reopen", http.MethodPost, "/cases/" + id + "/reopen", map[string]any{"actor": "Lin", "reason": "terminal retry"}},
			{"seal", http.MethodPost, "/cases/" + id + "/seal", map[string]any{"reviewer": "Mira", "summary": "terminal retry"}},
		}
		for _, probe := range probes {
			res, err := d.h.do(probe.method, probe.path, probe.body)
			if err != nil {
				d.t.Errorf("sealed case %s %s retry transport: %v", id, probe.name, err)
				continue
			}
			if res.status != http.StatusConflict {
				d.t.Errorf("sealed case %s %s retry: status=%d code=%s, want 409",
					id, probe.name, res.status, res.code)
			}
		}
		after := d.readDetail(id)
		d.compareDetail(fmt.Sprintf("sealed case %s terminal retries must not change any fact", id), after, before)
	}
}
