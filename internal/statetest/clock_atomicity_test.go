package statetest_test

import (
	"net/http"
	"testing"
)

// TestSealCommitTimestampAtomicity is the minimal reproduction of the single
// production deviation the model-driven suite finds. The specification states
// (PROJECT_SPEC.md, invariant: "Every mutation that changes a case also
// appends an audit event in the same repository commit") that the case state,
// the evidence seal and the sealing audit event are one commit. An atomic
// commit has one decision time: case.sealed_at, seal.sealed_at and the
// case_sealed event's created_at must therefore be the same instant.
//
// Observed phenomenon (do NOT weaken this assertion to make the suite green):
// internal/assurance/service.go SealCase calls s.now() three separate times -
// once for item.Seal, once for domain.NewEvidenceSeal and once for
// domain.NewAuditEvent. On this machine each sealing commit reports three
// instants a few hundred nanoseconds to about a microsecond apart. The other
// workflows (open, measure, reopen) call s.now() once per fact group and stay
// consistent; only sealing fans out.
//
// Minimal reproduction (also runnable in isolation):
//
//	go test ./internal/statetest/ -run TestSealCommitTimestampAtomicity -v
//
// Expected once the deviation is fixed: the three Equal checks pass and this
// test moves from FAIL to PASS without any edit to the test.
func TestSealCommitTimestampAtomicity(t *testing.T) {
	h := newHarness(t)
	d := newDriver(t, h, 9999)
	_, caseID := d.setupAwaiting("MET-RB-CLOCK")
	d.commitMeasurement(caseID, 25.2, "Rui") // review_ready

	res, err := d.h.do(http.MethodPost, "/cases/"+caseID+"/seal", map[string]any{
		"reviewer": "Anke",
		"summary":  "atomic commit timestamp probe",
	})
	if err != nil {
		t.Fatalf("seal transport: %v", err)
	}
	if res.status != http.StatusCreated {
		t.Fatalf("seal status=%d body=%s", res.status, res.raw)
	}
	if res.detail == nil || res.detail.Seal == nil {
		t.Fatalf("seal response missing detail/seal: %s", res.raw)
	}

	caseSealedAt := res.detail.Case.SealedAt
	caseUpdatedAt := res.detail.Case.UpdatedAt
	sealAt := res.detail.Seal.SealedAt
	if len(res.detail.Events) == 0 {
		t.Fatal("seal response carries no events")
	}
	lastEvent := res.detail.Events[len(res.detail.Events)-1]
	eventAt := lastEvent.CreatedAt

	t.Logf("sealing commit timestamps for %s:", caseID)
	t.Logf("  case.sealed_at           = %s", caseSealedAt)
	t.Logf("  case.updated_at         = %s", caseUpdatedAt)
	t.Logf("  seal.sealed_at          = %s", sealAt)
	t.Logf("  case_sealed.created_at  = %s (event %s)", eventAt, lastEvent.ID)
	t.Logf("  skew case->seal         = %v", sealAt.Sub(caseSealedAt))
	t.Logf("  skew case->event        = %v", eventAt.Sub(caseSealedAt))

	if !caseSealedAt.Equal(sealAt) {
		t.Errorf("case.sealed_at=%s != seal.sealed_at=%s: one sealing commit produced two instants (%v apart)",
			caseSealedAt, sealAt, sealAt.Sub(caseSealedAt))
	}
	if !caseUpdatedAt.Equal(sealAt) {
		t.Errorf("case.updated_at=%s != seal.sealed_at=%s: one sealing commit produced two instants (%v apart)",
			caseUpdatedAt, sealAt, sealAt.Sub(caseUpdatedAt))
	}
	if lastEvent.Kind != evCaseSealed || !eventAt.Equal(sealAt) {
		t.Errorf("sealing audit event is not stamped with the commit instant: kind=%s event_at=%s seal_at=%s",
			lastEvent.Kind, eventAt, sealAt)
	}

	// The same skew must be visible through the independent read endpoints;
	// this rules out a response-construction artifact and proves it is a
	// persisted fact.
	read := d.readDetail(caseID)
	if !read.Case.SealedAt.Equal(read.Seal.SealedAt) {
		t.Errorf("GET /cases/%s persists the skew: case.sealed_at=%s seal.sealed_at=%s",
			caseID, read.Case.SealedAt, read.Seal.SealedAt)
	}
}
