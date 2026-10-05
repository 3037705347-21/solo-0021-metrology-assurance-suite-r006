package statetest_test

import (
	"fmt"
	"net/http"
	"reflect"
	"time"
)

// normalizeTimes returns a copy of the detail in which every timestamp is set
// to the zero value. The oracle predicts no wall-clock values, so structural
// reconciliation compares everything except times; temporal relations are
// asserted separately on the observed view by assertTemporalRelations. Slice
// headers are left untouched so JSON null ("measurements":null) stays
// distinguishable from an empty array, the exact distinction that reveals a
// partially initialized response.
func normalizeTimes(in caseDetail) caseDetail {
	out := in
	out.Case.OpenedAt = time.Time{}
	out.Case.UpdatedAt = time.Time{}
	out.Case.SealedAt = time.Time{}
	normalizedMeasurements := make([]viewMeasurement, len(out.Measurements))
	for i := range out.Measurements {
		normalizedMeasurements[i] = out.Measurements[i]
		normalizedMeasurements[i].CreatedAt = time.Time{}
	}
	if out.Measurements != nil {
		out.Measurements = normalizedMeasurements
	}
	normalizedEvents := make([]viewEvent, len(out.Events))
	for i := range out.Events {
		normalizedEvents[i] = out.Events[i]
		normalizedEvents[i].CreatedAt = time.Time{}
	}
	if out.Events != nil {
		out.Events = normalizedEvents
	}
	if out.Seal != nil {
		seal := *out.Seal
		seal.SealedAt = time.Time{}
		out.Seal = &seal
	}
	return out
}

// compareDetail compares every persisted fact (case version/state, measurement
// history, seal and event stream) between an observed view and the oracle.
// Timestamp-bearing fields are normalized before comparison.
func (d *driver) compareDetail(context string, got, want caseDetail) {
	d.t.Helper()
	gn, wn := normalizeTimes(got), normalizeTimes(want)
	if !reflect.DeepEqual(gn, wn) {
		d.t.Errorf("[seed=%d step=%d] %s: case detail diverges from model\n  got : %+v\n  want: %+v",
			d.seed, d.step, context, gn, wn)
		d.describeDetailDiff(gn, wn)
	}
	d.assertTemporalRelations(context, got)
}

// describeDetailDiff narrows a deep-equal failure to the specific collection or
// field so the failure report is a minimal reproduction rather than two blobs.
func (d *driver) describeDetailDiff(got, want caseDetail) {
	d.t.Helper()
	if got.Case != want.Case {
		d.t.Errorf("  case field mismatch:\n    got : %+v\n    want: %+v", got.Case, want.Case)
	}
	if !reflect.DeepEqual(got.Measurements, want.Measurements) {
		d.t.Errorf("  measurement history mismatch (len got=%d want=%d)", len(got.Measurements), len(want.Measurements))
		d.describeMeasurementDiff(got.Measurements, want.Measurements)
	}
	if !reflect.DeepEqual(got.Seal, want.Seal) {
		d.t.Errorf("  seal mismatch:\n    got : %+v\n    want: %+v", got.Seal, want.Seal)
	}
	if !reflect.DeepEqual(got.Events, want.Events) {
		d.t.Errorf("  event stream mismatch (len got=%d want=%d)", len(got.Events), len(want.Events))
		d.describeEventDiff(got.Events, want.Events)
	}
}

func (d *driver) describeMeasurementDiff(got, want []viewMeasurement) {
	d.t.Helper()
	seen := map[string]viewMeasurement{}
	for _, m := range got {
		seen[m.ID] = m
	}
	for _, w := range want {
		g, ok := seen[w.ID]
		if !ok {
			d.t.Errorf("    measurement %s missing from observed view: %+v", w.ID, w)
			continue
		}
		if !reflect.DeepEqual(g, w) {
			d.t.Errorf("    measurement %s mismatch:\n      got : %+v\n      want: %+v", w.ID, g, w)
		}
	}
	for _, g := range got {
		if _, ok := lookupMeasurement(want, g.ID); !ok {
			d.t.Errorf("    ghost measurement present after rejected/rolled-back mutation: %+v", g)
		}
	}
}

func lookupMeasurement(items []viewMeasurement, id string) (viewMeasurement, bool) {
	for _, item := range items {
		if item.ID == id {
			return item, true
		}
	}
	return viewMeasurement{}, false
}

func (d *driver) describeEventDiff(got, want []viewEvent) {
	d.t.Helper()
	seen := map[string]viewEvent{}
	for _, e := range got {
		seen[e.ID] = e
	}
	for _, w := range want {
		g, ok := seen[w.ID]
		if !ok {
			d.t.Errorf("    event %s (%s) missing from observed stream", w.ID, w.Kind)
			continue
		}
		if !reflect.DeepEqual(g, w) {
			d.t.Errorf("    event %s mismatch:\n      got : %+v\n      want: %+v", w.ID, g, w)
		}
	}
	for _, g := range got {
		if _, ok := lookupEvent(want, g.ID); !ok {
			d.t.Errorf("    ghost event present after rejected/rolled-back mutation: id=%s kind=%s", g.ID, g.Kind)
		}
	}
}

func lookupEvent(items []viewEvent, id string) (viewEvent, bool) {
	for _, item := range items {
		if item.ID == id {
			return item, true
		}
	}
	return viewEvent{}, false
}

// assertTemporalRelations checks the timestamp invariants that any committed
// fact must satisfy: real (non-zero) stamps, monotonic append order, and
// equality of the terminal seal timestamps. These hold regardless of the
// unpredictable wall-clock value.
func (d *driver) assertTemporalRelations(context string, detail caseDetail) {
	d.t.Helper()
	cid := detail.Case.ID
	if detail.Case.OpenedAt.IsZero() || detail.Case.UpdatedAt.IsZero() {
		d.t.Errorf("%s case %s: zero case timestamp", context, cid)
	}
	var last time.Time
	for i, m := range detail.Measurements {
		if m.CreatedAt.IsZero() {
			d.t.Errorf("%s case %s measurement %s: zero timestamp", context, cid, m.ID)
		}
		if i > 0 && m.CreatedAt.Before(last) {
			d.t.Errorf("%s case %s: measurement timestamps out of append order", context, cid)
		}
		last = m.CreatedAt
	}
	last = time.Time{}
	for i, e := range detail.Events {
		if e.CreatedAt.IsZero() {
			d.t.Errorf("%s case %s event %s: zero timestamp", context, cid, e.ID)
		}
		if i > 0 && e.CreatedAt.Before(last) {
			d.t.Errorf("%s case %s: event timestamps out of append order", context, cid)
		}
		last = e.CreatedAt
	}
	if detail.Case.State == stSealed {
		if detail.Seal == nil {
			d.t.Errorf("%s case %s: sealed without seal fact", context, cid)
			return
		}
		if detail.Seal.SealedAt.IsZero() {
			d.t.Errorf("%s case %s: zero seal timestamp", context, cid)
		}
		// Spec invariant: case, seal and the sealing event are produced in the
		// same repository commit. The service stamps them with separate time.Now
		// calls, so strict equality is the atomicity witness; a skew (observed in
		// production at sub-microsecond scale) is reported as a deviation.
		if d.checkCommitClock {
			if !detail.Case.SealedAt.Equal(detail.Seal.SealedAt) {
				d.t.Errorf("%s case %s: case.sealed_at=%s != seal.sealed_at=%s (same-commit facts must share a timestamp)",
					context, cid, detail.Case.SealedAt, detail.Seal.SealedAt)
			}
			if !detail.Case.UpdatedAt.Equal(detail.Seal.SealedAt) {
				d.t.Errorf("%s case %s: case.updated_at=%s != seal.sealed_at=%s (same-commit facts must share a timestamp)",
					context, cid, detail.Case.UpdatedAt, detail.Seal.SealedAt)
			}
			lastEvent := detail.Events[len(detail.Events)-1]
			if lastEvent.Kind != evCaseSealed || !lastEvent.CreatedAt.Equal(detail.Seal.SealedAt) {
				d.t.Errorf("%s case %s: sealing commit is not atomic across case/seal/event timestamps (case=%s seal=%s event=%s)",
					context, cid, detail.Case.SealedAt, detail.Seal.SealedAt, lastEvent.CreatedAt)
			}
		}
	}
}

// assertStateInvariants checks the documented state-machine invariants against
// one observed detail, independent of what the oracle predicted.
func (d *driver) assertStateInvariants(detail caseDetail) {
	d.t.Helper()
	c := detail.Case

	// Version moves exactly once per committed case mutation.
	if c.Version != len(detail.Events) {
		d.t.Errorf("case %s: version=%d but event count=%d (every commit must bump both together)",
			c.ID, c.Version, len(detail.Events))
	}

	accepted, rejected, reopens := 0, 0, 0
	var kinds []string
	for _, e := range detail.Events {
		kinds = append(kinds, e.Kind)
		switch e.Kind {
		case evMeasurementOK:
			accepted++
		case evMeasurementBad:
			rejected++
		case evCaseReopened:
			reopens++
		}
		if e.CaseID != c.ID {
			d.t.Errorf("case %s: event %s carries foreign case id %q", c.ID, e.ID, e.CaseID)
		}
	}
	if accepted+rejected != len(detail.Measurements) {
		d.t.Errorf("case %s: %d measurements but %d measurement events", c.ID, len(detail.Measurements), accepted+rejected)
	}
	if reopens > rejected {
		d.t.Errorf("case %s: %d reopen events with only %d rejected measurements", c.ID, reopens, rejected)
	}

	for _, m := range detail.Measurements {
		wantAccepted := absFloat(m.Observed-m.Nominal) <= m.Tolerance
		if m.Accepted != wantAccepted {
			d.t.Errorf("case %s measurement %s: accepted=%v but |observed-nominal|=%v tolerance=%v",
				c.ID, m.ID, m.Accepted, absFloat(m.Observed-m.Nominal), m.Tolerance)
		}
		if m.Deviation != m.Observed-m.Nominal {
			d.t.Errorf("case %s measurement %s: deviation %v != observed-nominal %v",
				c.ID, m.ID, m.Deviation, m.Observed-m.Nominal)
		}
		if m.CaseID != c.ID || m.Nominal != c.Nominal || m.Tolerance != c.Tolerance {
			d.t.Errorf("case %s measurement %s: embedded case parameters diverge from case", c.ID, m.ID)
		}
	}

	switch c.State {
	case stAwaiting:
		// Opening or a reopen is the latest case-level event.
		if last := kinds[len(kinds)-1]; last != evCaseOpened && last != evCaseReopened {
			d.t.Errorf("case %s: state awaiting_measurement but last event is %q", c.ID, last)
		}
		if detail.Seal != nil {
			d.t.Errorf("case %s: awaiting case carries a seal", c.ID)
		}
	case stRecheck:
		if last := kinds[len(kinds)-1]; last != evMeasurementBad {
			d.t.Errorf("case %s: state recheck_required but last event is %q", c.ID, last)
		}
		if detail.Seal != nil {
			d.t.Errorf("case %s: recheck case carries a seal", c.ID)
		}
	case stReview:
		if accepted != 1 {
			d.t.Errorf("case %s: review_ready requires exactly one accepted measurement, found %d", c.ID, accepted)
		}
		if last := kinds[len(kinds)-1]; last != evMeasurementOK {
			d.t.Errorf("case %s: review_ready but last event is %q", c.ID, last)
		}
		if detail.Seal != nil {
			d.t.Errorf("case %s: review_ready case already carries a seal", c.ID)
		}
	case stSealed:
		if detail.Seal == nil {
			d.t.Errorf("case %s: sealed state without seal fact", c.ID)
		} else {
			best, ok := bestAccepted(detail.Measurements)
			if !ok {
				d.t.Errorf("case %s: sealed without any accepted measurement", c.ID)
			} else if detail.Seal.MeasurementID != best.ID {
				d.t.Errorf("case %s: seal cites %s but minimum-deviation accepted measurement is %s",
					c.ID, detail.Seal.MeasurementID, best.ID)
			}
			if detail.Seal.CaseID != c.ID || detail.Seal.DeviceID != c.DeviceID {
				d.t.Errorf("case %s: seal linkage diverges (seal=%+v)", c.ID, detail.Seal)
			}
		}
		if last := kinds[len(kinds)-1]; last != evCaseSealed {
			d.t.Errorf("case %s: sealed but last event is %q", c.ID, last)
		}
		if c.SealedAt.IsZero() {
			d.t.Errorf("case %s: sealed without sealed_at", c.ID)
		}
	default:
		d.t.Errorf("case %s: unknown state %q", c.ID, c.State)
	}

	// Event ordering must be a legal walk of the documented state machine.
	if err := legalEventWalk(kinds); err != nil {
		d.t.Errorf("case %s: event sequence %v is not a legal state-machine walk: %v", c.ID, kinds, err)
	}
}

// legalEventWalk replays kinds against the spec's transition graph.
func legalEventWalk(kinds []string) error {
	state := ""
	for i, kind := range kinds {
		switch kind {
		case evCaseOpened:
			if i != 0 {
				return fmt.Errorf("case_opened at position %d", i)
			}
			state = stAwaiting
		case evMeasurementOK:
			if state != stAwaiting {
				return fmt.Errorf("accepted measurement from %s", state)
			}
			state = stReview
		case evMeasurementBad:
			if state != stAwaiting {
				return fmt.Errorf("rejected measurement from %s", state)
			}
			state = stRecheck
		case evCaseReopened:
			if state != stRecheck {
				return fmt.Errorf("reopen from %s", state)
			}
			state = stAwaiting
		case evCaseSealed:
			if state != stReview {
				return fmt.Errorf("seal from %s", state)
			}
			state = stSealed
		default:
			return fmt.Errorf("unknown event kind %q", kind)
		}
	}
	return nil
}

// reconcileCase re-reads one case through both read endpoints and requires the
// views to agree with the oracle and with each other.
func (d *driver) reconcileCase(caseID, context string) {
	d.t.Helper()
	want, ok := d.m.cases[caseID]
	if !ok {
		d.t.Errorf("%s: oracle has no case %s", context, caseID)
		return
	}
	detail := d.readDetail(caseID)
	d.compareDetail(context, detail, want.detail)
	d.assertStateInvariants(detail)
	d.reconcileEvents(caseID, want.detail.Events)
}

// reconcileEvents proves the dedicated events endpoint returns exactly the
// same stream embedded in the detail view (same ids, kinds, order, count).
func (d *driver) reconcileEvents(caseID string, want []viewEvent) {
	d.t.Helper()
	res := d.mustDo(http.MethodGet, "/cases/"+caseID+"/events", nil)
	if res.status != http.StatusOK {
		d.t.Errorf("GET /cases/%s/events: status=%d body=%s", caseID, res.status, res.raw)
		return
	}
	if len(res.events) != len(want) {
		d.t.Errorf("case %s: events endpoint returned %d events, model has %d", caseID, len(res.events), len(want))
		return
	}
	for i, e := range res.events {
		wn := want[i]
		wn.CreatedAt = time.Time{}
		got := e
		got.CreatedAt = time.Time{}
		if !reflect.DeepEqual(got, wn) {
			d.t.Errorf("case %s events[%d]:\n  got : %+v\n  want: %+v", caseID, i, got, wn)
		}
		if e.CreatedAt.IsZero() {
			d.t.Errorf("case %s events[%d]: zero timestamp", caseID, i)
		}
	}
}

// reconcileDevice re-reads a device and compares it to the oracle.
func (d *driver) reconcileDevice(deviceID, context string) {
	d.t.Helper()
	want, ok := d.m.devices[deviceID]
	if !ok {
		d.t.Errorf("%s: oracle has no device %s", context, deviceID)
		return
	}
	res := d.mustDo(http.MethodGet, "/devices/"+deviceID, nil)
	if res.status != http.StatusOK || res.device == nil {
		d.t.Errorf("%s: GET device %s status=%d body=%s", context, deviceID, res.status, res.raw)
		return
	}
	got := *res.device
	got.EnrolledAt = time.Time{}
	wantView := want.device
	wantView.EnrolledAt = time.Time{}
	if !reflect.DeepEqual(got, wantView) {
		d.t.Errorf("%s: device %s diverges:\n  got : %+v\n  want: %+v", context, deviceID, got, wantView)
	}
}

// reconcileEverything reads every device and every case from the service and
// reconciles the whole world against the oracle, plus cross-resource
// invariants that require global knowledge.
func (d *driver) reconcileEverything(context string) {
	d.t.Helper()
	for _, id := range d.m.deviceIDs() {
		d.reconcileDevice(id, context)
	}

	unfinishedByDevice := map[string]string{}
	sealOwners := map[string]string{} // seal id -> case id (global uniqueness)
	for _, id := range d.m.caseIDs() {
		d.reconcileCase(id, context)
		c := d.m.cases[id].detail
		if c.Case.State != stSealed {
			if other, exists := unfinishedByDevice[c.Case.DeviceID]; exists {
				d.t.Errorf("%s: device %s has two unfinished cases %s and %s",
					context, c.Case.DeviceID, other, id)
			}
			unfinishedByDevice[c.Case.DeviceID] = id
		}
		if c.Seal != nil {
			if owner, exists := sealOwners[c.Seal.ID]; exists {
				d.t.Errorf("%s: seal id %s shared by cases %s and %s", context, c.Seal.ID, owner, id)
			}
			sealOwners[c.Seal.ID] = id
		}
	}
}
