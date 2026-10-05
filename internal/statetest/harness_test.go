// Package statetest contains model-driven black-box tests that drive the public
// HTTP API through the real handler stack (bootstrap -> httpapi -> assurance ->
// repository). Every test runs against an httptest.Server over a real TCP
// socket, so concurrent goroutines interleave inside the server exactly as they
// would in production.
//
// The suite is deterministic: operation sequences come from math/rand sources
// seeded with fixed constants, so a clean `go test ./...` checkout produces the
// same interleavings and the same evidence on every machine.
package statetest_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"example.com/solo-0021-metrology-assurance-suite/internal/bootstrap"
)

// rpcResult is the decoded envelope of one public API call.
type rpcResult struct {
	status int
	raw    []byte
	code   string // stable error code from {"error":{"code":...}}
	detail *caseDetail
	device *viewDevice
	events []viewEvent
}

// harness owns one fresh service instance per test.
type harness struct {
	t      *testing.T
	server *httptest.Server
	client *http.Client
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	server := httptest.NewServer(bootstrap.NewHandler())
	t.Cleanup(server.Close)
	return &harness{
		t:      t,
		server: server,
		client: server.Client(),
	}
}

// do issues one request. body is marshaled as JSON; raw sends an already
// encoded (possibly malformed) payload. The envelope is decoded just enough to
// expose the resource view and the stable error code. This method never calls
// t.Fatalf, so it is safe to invoke from concurrent goroutines; callers decide
// how to report a returned error.
func (h *harness) do(method, path string, body any, raw ...[]byte) (rpcResult, error) {
	h.t.Helper()

	var reader io.Reader
	hasRaw := len(raw) > 0 && raw[0] != nil
	if hasRaw {
		reader = bytes.NewReader(raw[0])
	} else if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return rpcResult{}, fmt.Errorf("marshal request body: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}

	request, err := http.NewRequest(method, h.server.URL+path, reader)
	if err != nil {
		return rpcResult{}, fmt.Errorf("build request: %w", err)
	}
	if body != nil || hasRaw {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := h.client.Do(request)
	if err != nil {
		return rpcResult{}, fmt.Errorf("%s %s transport: %w", method, path, err)
	}
	defer response.Body.Close()

	payload, err := io.ReadAll(response.Body)
	if err != nil {
		return rpcResult{}, fmt.Errorf("read response body: %w", err)
	}

	result := rpcResult{status: response.StatusCode, raw: payload}
	if len(bytes.TrimSpace(payload)) == 0 {
		return result, nil
	}

	var envelope struct {
		Error *struct {
			Code string `json:"code"`
		} `json:"error"`
		Device *viewDevice `json:"device"`
		Case   *struct {
			ID string `json:"id"`
		} `json:"case"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return rpcResult{}, fmt.Errorf("decode envelope from %s %s (%d): %w\nbody: %s", method, path, response.StatusCode, err, payload)
	}
	if envelope.Error != nil {
		result.code = envelope.Error.Code
		return result, nil
	}

	// Success envelopes: dispatch on route shape and re-decode into the full
	// view structures.
	switch {
	case method == http.MethodGet && path == "/healthz":
		return result, nil
	case envelope.Device != nil:
		var decoded struct {
			Device viewDevice `json:"device"`
		}
		if err := json.Unmarshal(payload, &decoded); err != nil {
			return rpcResult{}, fmt.Errorf("decode device envelope: %w", err)
		}
		result.device = &decoded.Device
	case hasEventsSuffix(path):
		var decoded struct {
			Events []viewEvent `json:"events"`
		}
		if err := json.Unmarshal(payload, &decoded); err != nil {
			return rpcResult{}, fmt.Errorf("decode events envelope: %w", err)
		}
		result.events = decoded.Events
	case envelope.Case != nil:
		var detail caseDetail
		if err := json.Unmarshal(payload, &detail); err != nil {
			return rpcResult{}, fmt.Errorf("decode case envelope: %w", err)
		}
		result.detail = &detail
	}
	return result, nil
}

func hasEventsSuffix(path string) bool {
	const suffix = "/events"
	return len(path) >= len(suffix) && path[len(path)-len(suffix):] == suffix
}

// status constants named after the role they play in an assertion.
const (
	statusOK       = http.StatusOK
	statusCreated  = http.StatusCreated
	statusBad      = http.StatusBadRequest
	statusNotFound = http.StatusNotFound
	statusConflict = http.StatusConflict
)
