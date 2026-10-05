package bootstrap_test

import (
	"bytes"
	"net/http"
	"testing"

	"example.com/solo-0021-metrology-assurance-suite/internal/contracts"
)

func assertStatus(t *testing.T, r httpResult, want int) {
	t.Helper()
	if r.status != want {
		t.Fatalf("status=%d want %d body=%s", r.status, want, r.raw)
	}
}

func assertStatusCode(t *testing.T, r httpResult, wantStatus int, wantCode, context string) {
	t.Helper()
	if r.status != wantStatus || (wantCode != "" && errorCode(r.raw) != wantCode) {
		t.Fatalf("%s: status=%d code=%q want %d %q body=%s",
			context, r.status, errorCode(r.raw), wantStatus, wantCode, r.raw)
	}
}

// assertUnchangedRead re-GETs the path and requires a byte-identical response.
// The encoder is deterministic, so any committed change shows up here.
func assertUnchangedRead(t *testing.T, e *env, path string, before []byte) {
	t.Helper()
	after := e.do(t, http.MethodGet, path, nil)
	if !bytes.Equal(bytes.TrimSpace(before), bytes.TrimSpace(after.raw)) {
		t.Fatalf("read of %s changed:\nbefore=%s\nafter =%s", path, before, after.raw)
	}
}

func readCase(t *testing.T, e *env, caseID string) contracts.CaseResponse {
	t.Helper()
	r := e.do(t, http.MethodGet, "/cases/"+caseID, nil)
	assertStatus(t, r, http.StatusOK)
	var view contracts.CaseResponse
	mustDecode(t, r.raw, &view)
	return view
}

func extractDeviceID(t *testing.T, r httpResult) string {
	t.Helper()
	var view contracts.DeviceResponse
	mustDecode(t, r.raw, &view)
	if view.Device.ID == "" {
		t.Fatalf("response has no device id: %s", r.raw)
	}
	return view.Device.ID
}

func extractCaseID(t *testing.T, r httpResult) string {
	t.Helper()
	var view contracts.CaseResponse
	mustDecode(t, r.raw, &view)
	if view.Case.ID == "" {
		t.Fatalf("response has no case id: %s", r.raw)
	}
	return view.Case.ID
}
