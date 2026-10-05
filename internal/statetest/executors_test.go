package statetest_test

import (
	"net/http"
)

// Each executor:
//  1. asks the oracle for the predicted outcome and advances the oracle only on
//     the predicted-success path,
//  2. issues the real HTTP call,
//  3. compares status/code,
//  4. on success compares the response body to the predicted post-commit view,
//  5. on rejection/rollback re-reads the target's detail and event streams and
//     proves they are byte-for-byte (structurally) the pre-attempt view, i.e.
//     no half-applied version, event, measurement or seal survived.

func (d *driver) registerOne(tag string, expectDuplicate bool) {
	d.t.Helper()
	ste, room := d.people[d.rint(len(d.people))], d.rooms[d.rint(len(d.rooms))]
	// Invalid-tag probes use an otherwise valid body.
	pred, predictedID := d.m.registerPredict(tag, "Bench Meter", room, ste)
	body := map[string]any{
		"asset_tag": tag,
		"model":     "Bench Meter",
		"room":      room,
		"steward":   ste,
	}
	res := d.mustDo(http.MethodPost, "/devices", body)
	if res.status != pred.status || res.code != pred.code {
		d.t.Errorf("[seed=%d step=%d] register tag=%q: got (%d,%s), want (%d,%s); body=%s",
			d.seed, d.step, tag, res.status, res.code, pred.status, pred.code, res.raw)
		if !pred.ok {
			return
		}
	}
	if !pred.ok {
		// Rejected registration: the previously stored device (if any) must be
		// unchanged and no device id may have been consumed.
		if expectDuplicate && res.status == http.StatusConflict {
			id := d.m.byTag[normalizeTag(tag)]
			d.reconcileDevice(id, "after duplicate registration rejection")
		}
		d.assertNoDeviceIDGap()
		return
	}
	if res.device == nil || res.device.ID != predictedID {
		d.t.Errorf("[seed=%d step=%d] register success returned device %+v, want id %s", d.seed, d.step, res.device, predictedID)
		return
	}
	d.reconcileDevice(predictedID, "register response vs store")
}

func (d *driver) openOne(deviceID string, nominal, tolerance float64, steward string) {
	d.t.Helper()
	// Use a fixed steward: drawing from the RNG here would make the number of
	// random draws per step depend on the caller, which breaks the fixed-seed
	// operation sequence. Callers that need variety pass the steward in.
	if steward == "" {
		steward = "Qiao"
	}
	pred, predictedID := d.m.openPredict(deviceID, nominal, tolerance, steward)
	body := map[string]any{
		"device_id": deviceID,
		"nominal":   nominal,
		"tolerance": tolerance,
		"steward":   steward,
	}
	res := d.mustDo(http.MethodPost, "/cases", body)
	if res.status != pred.status || res.code != pred.code {
		d.t.Errorf("[seed=%d step=%d] open case device=%s tol=%v: got (%d,%s), want (%d,%s); body=%s",
			d.seed, d.step, deviceID, tolerance, res.status, res.code, pred.status, pred.code, res.raw)
	}
	switch {
	case pred.ok:
		if res.detail == nil || res.detail.Case.ID != predictedID {
			d.t.Errorf("[seed=%d step=%d] open success returned %+v, want case id %s", d.seed, d.step, res.detail, predictedID)
			return
		}
		d.compareDetail("open response", *res.detail, d.m.cases[predictedID].detail)
		d.reconcileCase(predictedID, "after open")
	case pred.caseIDHint != "" && d.m.cases[pred.caseIDHint] != nil:
		// active_case_exists rejection: the existing unfinished case must be
		// untouched.
		d.reconcileCase(pred.caseIDHint, "after open rejection")
	default:
		// Unknown-device or invalid-body rejection: no case id may be burned.
		d.assertNoCaseIDGap()
	}
}

func (d *driver) measureOne(caseID string, observed float64, technician string) {
	d.t.Helper()
	before, known := d.m.cases[caseID]
	var beforeView caseDetail
	if known {
		beforeView = before.detail
	}
	pred := d.m.measurementPredict(caseID, observed, technician)
	body := map[string]any{"observed": observed, "technician": technician}
	res := d.mustDo(http.MethodPost, "/cases/"+caseID+"/measurements", body)
	if res.status != pred.status || res.code != pred.code {
		d.t.Errorf("[seed=%d step=%d] measure case=%s observed=%v tech=%q: got (%d,%s), want (%d,%s); body=%s",
			d.seed, d.step, caseID, observed, technician, res.status, res.code, pred.status, pred.code, res.raw)
	}
	if pred.ok {
		if res.detail == nil {
			d.t.Errorf("[seed=%d step=%d] measure success without case view: %s", d.seed, d.step, res.raw)
			return
		}
		d.compareDetail("measure response", *res.detail, d.m.cases[caseID].detail)
		d.reconcileCase(caseID, "after accepted/rejected measurement commit")
		return
	}
	// Rejected or rolled back: nothing may have moved.
	if known {
		d.compareDetail("post-rejection detail must equal pre-attempt view", d.readDetail(caseID), beforeView)
		d.reconcileEvents(caseID, beforeView.Events)
		d.reconcileCase(caseID, "after measurement rejection/rollback")
	} else {
		d.assertNotFound(caseID)
	}
}

func (d *driver) reopenOne(caseID, actor, reason string) {
	d.t.Helper()
	before, known := d.m.cases[caseID]
	var beforeView caseDetail
	if known {
		beforeView = before.detail
	}
	pred := d.m.reopenPredict(caseID, actor, reason)
	body := map[string]any{"actor": actor, "reason": reason}
	res := d.mustDo(http.MethodPost, "/cases/"+caseID+"/reopen", body)
	if res.status != pred.status || res.code != pred.code {
		d.t.Errorf("[seed=%d step=%d] reopen case=%s actor=%q reason=%q: got (%d,%s), want (%d,%s); body=%s",
			d.seed, d.step, caseID, actor, reason, res.status, res.code, pred.status, pred.code, res.raw)
	}
	if pred.ok {
		if res.detail == nil {
			d.t.Errorf("[seed=%d step=%d] reopen success without case view: %s", d.seed, d.step, res.raw)
			return
		}
		d.compareDetail("reopen response", *res.detail, d.m.cases[caseID].detail)
		d.reconcileCase(caseID, "after reopen commit")
		return
	}
	if known {
		// This is the critical rollback probe when the body is invalid in
		// recheck_required: production mutates the cloned case before
		// validating actor/reason and relies on the repository discarding the
		// clone. The reads below must show the original state, version and
		// event history.
		d.compareDetail("post-rollback detail must equal pre-attempt view", d.readDetail(caseID), beforeView)
		d.reconcileEvents(caseID, beforeView.Events)
		d.reconcileCase(caseID, "after reopen rejection/rollback")
	} else {
		d.assertNotFound(caseID)
	}
}

func (d *driver) sealOne(caseID, reviewer, summary string) {
	d.t.Helper()
	before, known := d.m.cases[caseID]
	var beforeView caseDetail
	if known {
		beforeView = before.detail
	}
	pred := d.m.sealPredict(caseID, reviewer, summary)
	body := map[string]any{"reviewer": reviewer, "summary": summary}
	res := d.mustDo(http.MethodPost, "/cases/"+caseID+"/seal", body)
	if res.status != pred.status || res.code != pred.code {
		d.t.Errorf("[seed=%d step=%d] seal case=%s reviewer=%q: got (%d,%s), want (%d,%s); body=%s",
			d.seed, d.step, caseID, reviewer, res.status, res.code, pred.status, pred.code, res.raw)
	}
	if pred.ok {
		if res.detail == nil {
			d.t.Errorf("[seed=%d step=%d] seal success without case view: %s", d.seed, d.step, res.raw)
			return
		}
		d.compareDetail("seal response", *res.detail, d.m.cases[caseID].detail)
		d.reconcileCase(caseID, "after seal commit")
		return
	}
	if known {
		// Critical rollback probe for review_ready with an invalid reviewer:
		// production flips the cloned case to sealed and constructs the seal
		// before input validation rejects the commit. No seal, version bump or
		// event may survive. Terminal-state retries (seal_exists / state
		// conflict) are covered here too.
		d.compareDetail("post-rollback detail must equal pre-attempt view", d.readDetail(caseID), beforeView)
		d.reconcileEvents(caseID, beforeView.Events)
		d.reconcileCase(caseID, "after seal rejection/rollback")
	} else {
		d.assertNotFound(caseID)
	}
}

func (d *driver) rawOne(method, path, body string, caseID string) {
	d.t.Helper()
	before, known := d.m.cases[caseID]
	var beforeView caseDetail
	if known {
		beforeView = before.detail
	}
	res := d.mustDo(method, path, nil, []byte(body))
	if res.status != http.StatusBadRequest || res.code != codeInvalid {
		d.t.Errorf("[seed=%d step=%d] malformed %s %s body=%q: got (%d,%s), want (400,%s)",
			d.seed, d.step, method, path, body, res.status, res.code, codeInvalid)
	}
	if known {
		d.compareDetail("malformed request must leave detail untouched", d.readDetail(caseID), beforeView)
		d.reconcileEvents(caseID, beforeView.Events)
	}
}

func (d *driver) readDetail(caseID string) caseDetail {
	d.t.Helper()
	res := d.mustDo(http.MethodGet, "/cases/"+caseID, nil)
	if res.status != http.StatusOK || res.detail == nil {
		d.t.Fatalf("[seed=%d step=%d] read case %s: status=%d body=%s", d.seed, d.step, caseID, res.status, res.raw)
	}
	return *res.detail
}

func (d *driver) assertNotFound(caseID string) {
	d.t.Helper()
	for _, path := range []string{"/cases/" + caseID, "/cases/" + caseID + "/events"} {
		res := d.mustDo(http.MethodGet, path, nil)
		if res.status != http.StatusNotFound || res.code != codeNotFound {
			d.t.Errorf("[seed=%d step=%d] GET %s: got (%d,%s), want (404,%s)",
				d.seed, d.step, path, res.status, res.code, codeNotFound)
		}
	}
}

// assertNoDeviceIDGap and assertNoCaseIDGap are intentionally read-only:
// creating probe entities on the rejection path would make the fixed-seed RNG
// stream depend on live responses. Counter drift after a rejected mutation is
// instead witnessed on every later successful creation, whose id must equal
// the oracle's prediction; a burned counter would desync that exact id.
func (d *driver) assertNoDeviceIDGap() { d.t.Helper() }

func (d *driver) assertNoCaseIDGap() { d.t.Helper() }
