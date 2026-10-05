package bootstrap_test

// Model-driven state machine tests for the metrology assurance service.
//
// The tests in this package drive the *real* HTTP handler through a real
// loopback HTTP server (no mocks, no in-process service calls) while a
// specification-level reference model predicts the outcome of every request.
// After each request - success or rejection - every known device and case is
// read back through the public detail and events endpoints and compared with
// the model, so that a rejected or rolled-back mutation can never leave a
// half-written version, measurement list, seal, or audit trail.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"example.com/solo-0021-metrology-assurance-suite/internal/bootstrap"
	"example.com/solo-0021-metrology-assurance-suite/internal/contracts"
	"example.com/solo-0021-metrology-assurance-suite/internal/domain"
)

// ---------------------------------------------------------------------------
// HTTP environment
// ---------------------------------------------------------------------------

type env struct {
	server *httptest.Server
	client *http.Client
}

func newEnv() *env {
	server := httptest.NewServer(bootstrap.NewHandler())
	return &env{server: server, client: server.Client()}
}

func (e *env) close() { e.server.Close() }

type httpResult struct {
	status int
	raw    []byte
}

func (e *env) do(t *testing.T, method, path string, body any) httpResult {
	t.Helper()
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(buf)
	}
	return e.doRaw(t, method, path, reader, body != nil)
}

func (e *env) doRaw(t *testing.T, method, path string, reader io.Reader, withJSON bool) httpResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, e.server.URL+path, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if withJSON {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := e.client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return httpResult{status: response.StatusCode, raw: raw}
}

// doScript sends a scripted request after rewriting __DEV_n__/__CASE_n__
// tokens in the path and JSON body to concrete resource ids.
func (e *env) doScript(t *testing.T, method, path string, body any, deviceIDs, caseIDs map[int]string) httpResult {
	t.Helper()
	path = resolveTokens(path, deviceIDs, caseIDs)
	if body == nil {
		return e.do(t, method, path, nil)
	}
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	buf = []byte(resolveTokens(string(buf), deviceIDs, caseIDs))
	return e.doRaw(t, method, path, bytes.NewReader(buf), true)
}

// ---------------------------------------------------------------------------
// Scripted operations
// ---------------------------------------------------------------------------

type opKind int

const (
	opRegister opKind = iota
	opRegisterInvalid
	opRegisterDuplicate
	opOpen
	opOpenInvalid
	opOpenUnknownDevice
	opMeasure
	opMeasureInvalidTechnician
	opMeasureUnknownCase
	opReopen
	opReopenInvalid
	opReopenUnknownCase
	opSeal
	opSealInvalid
	opSealUnknownCase
)

// step is one concrete, replayable request. Resources are referenced by stable
// symbols assigned when creation is planned, so a script can be replayed on a
// fresh server and re-referenced after earlier steps are removed. Symbol
// fields are -1 when unset; 0 is the first real symbol.
type step struct {
	kind opKind

	tag     string
	model   string
	room    string
	steward string
	note    string
	rawID   string

	deviceSym    int // >= 0 when the request targets a known device
	caseSym      int // >= 0 when the request targets a known case
	newDeviceSym int // >= 0 when this step is predicted to create a device
	newCaseSym   int // >= 0 when this step is predicted to create a case

	nominal    float64
	tolerance  float64
	observed   float64
	technician string
	actor      string
	reason     string
	reviewer   string
	summary    string

	wantStatus int
	wantCode   string
}

func newStep(kind opKind) *step {
	return &step{
		kind:         kind,
		deviceSym:    -1,
		caseSym:      -1,
		newDeviceSym: -1,
		newCaseSym:   -1,
	}
}

func (s *step) deepCopy() *step {
	clone := *s
	return &clone
}

func (s *step) predictedSuccess() bool {
	return s.wantStatus == http.StatusCreated || s.wantStatus == http.StatusOK
}

// request renders the planned request. Resource ids remain as placeholders
// resolved at execution time by doScript.
func (s *step) request() (method, path string, body map[string]any) {
	switch s.kind {
	case opRegister, opRegisterInvalid, opRegisterDuplicate:
		return http.MethodPost, "/devices", map[string]any{
			"asset_tag": s.tag,
			"model":     s.model,
			"room":      s.room,
			"steward":   s.steward,
		}
	case opOpen, opOpenInvalid:
		return http.MethodPost, "/cases", map[string]any{
			"device_id": deviceRef(s.deviceSym),
			"nominal":   s.nominal,
			"tolerance": s.tolerance,
			"steward":   s.steward,
		}
	case opOpenUnknownDevice:
		return http.MethodPost, "/cases", map[string]any{
			"device_id": s.rawID,
			"nominal":   s.nominal,
			"tolerance": s.tolerance,
			"steward":   s.steward,
		}
	case opMeasure, opMeasureInvalidTechnician:
		return http.MethodPost, "/cases/" + caseRef(s.caseSym) + "/measurements",
			map[string]any{"observed": s.observed, "technician": s.technician}
	case opMeasureUnknownCase:
		return http.MethodPost, "/cases/" + s.rawID + "/measurements",
			map[string]any{"observed": s.observed, "technician": s.technician}
	case opReopen, opReopenInvalid:
		return http.MethodPost, "/cases/" + caseRef(s.caseSym) + "/reopen",
			map[string]any{"actor": s.actor, "reason": s.reason}
	case opReopenUnknownCase:
		return http.MethodPost, "/cases/" + s.rawID + "/reopen",
			map[string]any{"actor": s.actor, "reason": s.reason}
	case opSeal, opSealInvalid:
		return http.MethodPost, "/cases/" + caseRef(s.caseSym) + "/seal",
			map[string]any{"reviewer": s.reviewer, "summary": s.summary}
	default: // opSealUnknownCase
		return http.MethodPost, "/cases/" + s.rawID + "/seal",
			map[string]any{"reviewer": s.reviewer, "summary": s.summary}
	}
}

func deviceRef(sym int) string { return fmt.Sprintf("__DEV_%d__", sym) }
func caseRef(sym int) string   { return fmt.Sprintf("__CASE_%d__", sym) }

// resolveTokens rewrites __DEV_n__/__CASE_n__ placeholders to concrete ids.
func resolveTokens(text string, deviceIDs, caseIDs map[int]string) string {
	text = replaceTokenKind(text, "DEV", deviceIDs)
	text = replaceTokenKind(text, "CASE", caseIDs)
	return text
}

func replaceTokenKind(text, kind string, ids map[int]string) string {
	prefix := "__" + kind + "_"
	for {
		start := strings.Index(text, prefix)
		if start < 0 {
			return text
		}
		digits := start + len(prefix)
		end := digits
		for end < len(text) && text[end] >= '0' && text[end] <= '9' {
			end++
		}
		if end == digits || end+2 > len(text) || text[end:end+2] != "__" {
			// Not a well-formed token; neutralize it to avoid an infinite loop.
			text = text[:start] + "?" + text[start+len(prefix):]
			continue
		}
		n := 0
		for i := digits; i < end; i++ {
			n = n*10 + int(text[i]-'0')
		}
		id, ok := ids[n]
		if !ok {
			return text
		}
		text = text[:start] + id + text[end+2:]
	}
}

// ---------------------------------------------------------------------------
// Reference (specification) model
// ---------------------------------------------------------------------------

const (
	stateAwaiting = string(domain.CaseAwaitingMeasurement)
	stateReview   = string(domain.CaseReviewReady)
	stateRecheck  = string(domain.CaseRecheckRequired)
	stateSealed   = string(domain.CaseSealed)
	statusActive  = string(domain.DeviceActive)
	maxDevices    = 6
)

type modelDevice struct {
	sym int
	id  string
	tag string
}

type modelMeasurement struct {
	id         string
	observed   float64
	accepted   bool
	technician string
}

type modelEvent struct {
	id    string
	kind  string
	actor string
}

type modelSeal struct {
	id            string
	measurementID string
	reviewer      string
}

type modelCase struct {
	sym          int
	id           string
	deviceSym    int
	nominal      float64
	tolerance    float64
	steward      string
	state        string
	version      int
	measurements []modelMeasurement
	events       []modelEvent
	seal         *modelSeal
}

type world struct {
	devices map[int]*modelDevice
	cases   map[int]*modelCase

	deviceOrder []int
	caseOrder   []int

	nextDeviceSym int
	nextCaseSym   int

	// Successful-allocation counters. Failed mutations roll back and must not
	// advance these; the generated ids are externally visible.
	deviceCountN      int
	caseCountN        int
	measurementCountN int
	sealCountN        int
	eventCountN       int
}

func newWorld() *world {
	return &world{devices: make(map[int]*modelDevice), cases: make(map[int]*modelCase)}
}

func fmtID(prefix string, n int) string { return fmt.Sprintf("%s-%04d", prefix, n) }

func normalizeTag(value string) string { return strings.ToUpper(strings.TrimSpace(value)) }

func (w *world) deviceCount() int { return len(w.devices) }

func (w *world) openCaseForDevice(deviceSym int) *modelCase {
	for _, sym := range w.caseOrder {
		c := w.cases[sym]
		if c.deviceSym == deviceSym && c.state != stateSealed {
			return c
		}
	}
	return nil
}

func (w *world) randomDevice(r *rand.Rand) *modelDevice {
	return w.devices[w.deviceOrder[r.Intn(len(w.deviceOrder))]]
}

func (w *world) randomCase(r *rand.Rand) *modelCase {
	return w.cases[w.caseOrder[r.Intn(len(w.caseOrder))]]
}

// freshTag builds the next unused tag from the committed device count. A
// rejected registration does not advance that count, so its tag stays free for
// the model to reuse.
func (w *world) freshTag() string { return fmt.Sprintf("TAG-%04d", w.deviceCountN+1) }

// apply advances the model with the predicted outcome. Rejected steps are
// no-ops: a rejection must leave no trace.
func (w *world) apply(s *step) {
	if !s.predictedSuccess() {
		return
	}
	switch s.kind {
	case opRegister:
		w.deviceCountN++
		w.eventCountN++ // device_enrolled is not attached to a case.
		sym := s.newDeviceSym
		w.devices[sym] = &modelDevice{sym: sym, id: fmtID("device", w.deviceCountN), tag: normalizeTag(s.tag)}
		w.deviceOrder = append(w.deviceOrder, sym)

	case opOpen:
		w.caseCountN++
		w.eventCountN++
		sym := s.newCaseSym
		w.cases[sym] = &modelCase{
			sym:       sym,
			id:        fmtID("case", w.caseCountN),
			deviceSym: s.deviceSym,
			nominal:   s.nominal,
			tolerance: s.tolerance,
			steward:   s.steward,
			state:     stateAwaiting,
			version:   1,
			events:    []modelEvent{{id: fmtID("event", w.eventCountN), kind: "case_opened", actor: s.steward}},
		}
		w.caseOrder = append(w.caseOrder, sym)

	case opMeasure:
		c := w.cases[s.caseSym]
		w.measurementCountN++
		w.eventCountN++
		accepted := math.Abs(s.observed-c.nominal) <= c.tolerance
		c.measurements = append(c.measurements, modelMeasurement{
			id: fmtID("measurement", w.measurementCountN), observed: s.observed,
			accepted: accepted, technician: s.technician,
		})
		kind := "measurement_rejected"
		if accepted {
			c.state = stateReview
			kind = "measurement_accepted"
		} else {
			c.state = stateRecheck
		}
		c.version++
		c.events = append(c.events, modelEvent{id: fmtID("event", w.eventCountN), kind: kind, actor: s.technician})

	case opReopen:
		c := w.cases[s.caseSym]
		w.eventCountN++
		c.state = stateAwaiting
		c.version++
		c.events = append(c.events, modelEvent{id: fmtID("event", w.eventCountN), kind: "case_reopened", actor: s.actor})

	case opSeal:
		c := w.cases[s.caseSym]
		w.sealCountN++
		w.eventCountN++
		best := c.measurements[0]
		bestDev := math.Abs(best.observed - c.nominal)
		for _, m := range c.measurements[1:] {
			if dev := math.Abs(m.observed - c.nominal); m.accepted && dev < bestDev {
				best, bestDev = m, dev
			}
		}
		c.state = stateSealed
		c.version++
		c.seal = &modelSeal{id: fmtID("seal", w.sealCountN), measurementID: best.id, reviewer: s.reviewer}
		c.events = append(c.events, modelEvent{id: fmtID("event", w.eventCountN), kind: "case_sealed", actor: s.reviewer})
	}
}

// ---------------------------------------------------------------------------
// Model-guided generation (deterministic given the seed)
// ---------------------------------------------------------------------------

var (
	technicians   = []string{"Rui", "Mira", "Kai"}
	stewards      = []string{"Lin", "Qiao"}
	rooms         = []string{"Room A", "Room B", "Lab 2"}
	models        = []string{"Bench Meter", "Reference Probe", "Field Gauge"}
	nominalVals   = []float64{10, 25, 100}
	toleranceVals = []float64{0.1, 0.5, 1, 2.5}
)

func pick[T any](r *rand.Rand, values []T) T { return values[r.Intn(len(values))] }

func planScript(seed int64, count int) []*step {
	r := rand.New(rand.NewSource(seed))
	w := newWorld()
	script := make([]*step, 0, count)
	for i := 0; i < count; i++ {
		s := w.plan(r)
		script = append(script, s)
		w.apply(s)
	}
	return script
}

func (w *world) plan(r *rand.Rand) *step {
	// Bootstrap: only registrations/openings are possible before resources exist.
	if w.deviceCount() == 0 {
		return w.planRegistration(r)
	}
	if len(w.caseOrder) == 0 {
		return w.planOpen(r)
	}
	switch roll := r.Intn(100); {
	case roll < 14:
		return w.planRegistration(r)
	case roll < 32:
		return w.planOpen(r)
	case roll < 55:
		return w.planMeasurement(r)
	case roll < 75:
		return w.planReopen(r)
	default:
		return w.planSeal(r)
	}
}

func (w *world) planRegistration(r *rand.Rand) *step {
	switch sub := r.Intn(100); {
	case w.deviceCount() == 0:
		// No device to duplicate against; only creation/invalid creation is possible.
		if sub < 78 {
			return w.planFreshRegistration(r)
		}
		return w.planInvalidRegistration(r)
	case w.deviceCount() >= maxDevices:
		return w.planDuplicateRegistration(r)
	case sub < 60:
		return w.planFreshRegistration(r)
	case sub < 78:
		return w.planDuplicateRegistration(r)
	default:
		return w.planInvalidRegistration(r)
	}
}

func (w *world) planInvalidRegistration(r *rand.Rand) *step {
	// Invalid registration with a fresh tag: rejected, must not reserve tag/id.
	s := newStep(opRegisterInvalid)
	s.tag = w.freshTag()
	s.model = pick(r, models)
	s.room = pick(r, rooms)
	s.steward = pick(r, stewards)
	s.wantStatus = http.StatusBadRequest
	s.wantCode = "invalid_input"
	s.note = "register device with missing required field"
	switch r.Intn(3) {
	case 0:
		s.model = ""
	case 1:
		s.room = ""
	default:
		s.steward = ""
	}
	return s
}

func (w *world) planFreshRegistration(r *rand.Rand) *step {
	tag := w.freshTag()
	if r.Intn(2) == 0 {
		tag = "  " + strings.ToLower(tag) + " "
	}
	s := newStep(opRegister)
	s.tag = tag
	s.model = pick(r, models)
	s.room = pick(r, rooms)
	s.steward = pick(r, stewards)
	s.newDeviceSym = w.nextDeviceSym
	s.wantStatus = http.StatusCreated
	s.note = "register new device"
	w.nextDeviceSym++
	return s
}

func (w *world) planDuplicateRegistration(r *rand.Rand) *step {
	existing := w.randomDevice(r)
	wrapped := existing.tag
	switch r.Intn(3) {
	case 0:
		wrapped = strings.ToLower(existing.tag)
	case 1:
		wrapped = " " + existing.tag + "  "
	default:
		wrapped = strings.ToLower(" " + existing.tag)
	}
	s := newStep(opRegisterDuplicate)
	s.tag = wrapped
	s.model = pick(r, models)
	s.room = pick(r, rooms)
	s.steward = pick(r, stewards)
	s.wantStatus = http.StatusConflict
	s.wantCode = "duplicate_asset_tag"
	s.note = "register duplicate normalized asset tag"
	return s
}

func (w *world) planOpen(r *rand.Rand) *step {
	if r.Intn(100) < 12 {
		s := newStep(opOpenUnknownDevice)
		s.rawID = "device-7777"
		s.nominal = 25
		s.tolerance = 0.5
		s.steward = pick(r, stewards)
		s.wantStatus = http.StatusNotFound
		s.wantCode = "not_found"
		s.note = "open case for unknown device"
		return s
	}
	device := w.randomDevice(r)
	if current := w.openCaseForDevice(device.sym); current != nil {
		s := newStep(opOpen)
		s.deviceSym = device.sym
		s.nominal = 25
		s.tolerance = 0.5
		s.steward = pick(r, stewards)
		s.wantStatus = http.StatusConflict
		s.wantCode = "active_case_exists"
		s.note = "open second unfinished case for device"
		return s
	}
	if r.Intn(100) < 25 {
		bad := 0.0
		if r.Intn(2) == 0 {
			bad = -1.5
		}
		s := newStep(opOpenInvalid)
		s.deviceSym = device.sym
		s.nominal = pick(r, nominalVals)
		s.tolerance = bad
		s.steward = pick(r, stewards)
		s.wantStatus = http.StatusBadRequest
		s.wantCode = "invalid_input"
		s.note = "open case with non-positive tolerance"
		return s
	}
	s := newStep(opOpen)
	s.deviceSym = device.sym
	s.nominal = pick(r, nominalVals)
	s.tolerance = pick(r, toleranceVals)
	s.steward = pick(r, stewards)
	s.newCaseSym = w.nextCaseSym
	s.wantStatus = http.StatusCreated
	s.note = "open new case"
	w.nextCaseSym++
	return s
}

func observedValue(r *rand.Rand, nominal, tolerance float64) float64 {
	sign := 1.0
	if r.Intn(2) == 1 {
		sign = -1
	}
	switch r.Intn(5) {
	case 0:
		return nominal
	case 1:
		return nominal + sign*tolerance*0.5
	case 2:
		return nominal + sign*tolerance // exact tolerance boundary: accepted
	case 3:
		return nominal + sign*(tolerance+0.01)
	default:
		return nominal + sign*(2*tolerance+0.25)
	}
}

func (w *world) planMeasurement(r *rand.Rand) *step {
	if r.Intn(100) < 12 {
		s := newStep(opMeasureUnknownCase)
		s.rawID = "case-7777"
		s.observed = 25.2
		s.technician = pick(r, technicians)
		s.wantStatus = http.StatusNotFound
		s.wantCode = "not_found"
		s.note = "record measurement for unknown case"
		return s
	}
	c := w.randomCase(r)
	if c.state == stateAwaiting && r.Intn(100) < 15 {
		s := newStep(opMeasureInvalidTechnician)
		s.caseSym = c.sym
		s.observed = observedValue(r, c.nominal, c.tolerance)
		s.technician = ""
		s.wantStatus = http.StatusBadRequest
		s.wantCode = "invalid_input"
		s.note = "record measurement without technician"
		return s
	}
	s := newStep(opMeasure)
	s.caseSym = c.sym
	s.observed = observedValue(r, c.nominal, c.tolerance)
	s.technician = pick(r, technicians)
	if c.state == stateAwaiting {
		s.wantStatus = http.StatusCreated
		s.note = "record measurement for awaiting case"
	} else {
		s.wantStatus = http.StatusConflict
		s.wantCode = "state_conflict"
		s.note = "record measurement against non-awaiting case"
	}
	return s
}

func (w *world) planReopen(r *rand.Rand) *step {
	if r.Intn(100) < 12 {
		s := newStep(opReopenUnknownCase)
		s.rawID = "case-7777"
		s.actor = pick(r, stewards)
		s.reason = "setup replaced"
		s.wantStatus = http.StatusNotFound
		s.wantCode = "not_found"
		s.note = "reopen unknown case"
		return s
	}
	c := w.randomCase(r)
	if c.state == stateRecheck && r.Intn(100) < 35 {
		s := newStep(opReopenInvalid)
		s.caseSym = c.sym
		s.actor = pick(r, stewards)
		s.reason = "setup replaced"
		s.wantStatus = http.StatusBadRequest
		s.wantCode = "invalid_input"
		s.note = "reopen with missing actor or reason"
		if r.Intn(2) == 0 {
			s.actor = ""
		} else {
			s.reason = "   "
		}
		return s
	}
	s := newStep(opReopen)
	s.caseSym = c.sym
	s.actor = pick(r, stewards)
	s.reason = "measurement setup was replaced"
	if c.state == stateRecheck {
		s.wantStatus = http.StatusOK
		s.note = "reopen recheck-required case"
	} else {
		s.wantStatus = http.StatusConflict
		s.wantCode = "state_conflict"
		s.note = "reopen case that is not recheck-required"
	}
	return s
}

func (w *world) planSeal(r *rand.Rand) *step {
	if r.Intn(100) < 10 {
		s := newStep(opSealUnknownCase)
		s.rawID = "case-7777"
		s.reviewer = "Verifier"
		s.summary = "review complete"
		s.wantStatus = http.StatusNotFound
		s.wantCode = "not_found"
		s.note = "seal unknown case"
		return s
	}
	c := w.randomCase(r)
	if c.state == stateReview && r.Intn(100) < 25 {
		s := newStep(opSealInvalid)
		s.caseSym = c.sym
		s.reviewer = "Verifier"
		s.summary = "review complete"
		s.wantStatus = http.StatusBadRequest
		s.wantCode = "invalid_input"
		s.note = "seal with missing reviewer or summary"
		switch r.Intn(3) {
		case 0:
			s.reviewer = ""
		case 1:
			s.summary = ""
		default:
			s.reviewer = " "
			s.summary = ""
		}
		return s
	}
	s := newStep(opSeal)
	s.caseSym = c.sym
	s.reviewer = fmt.Sprintf("Verifier-%d", r.Intn(1000))
	s.summary = "accepted measurement sealed after review"
	switch c.state {
	case stateReview:
		s.wantStatus = http.StatusCreated
		s.note = "seal review-ready case"
	case stateSealed:
		s.wantStatus = http.StatusConflict
		s.wantCode = "seal_exists"
		s.note = "repeat seal on sealed case"
	default:
		s.wantStatus = http.StatusConflict
		s.wantCode = "state_conflict"
		s.note = "seal case without a completed review"
	}
	return s
}

// ---------------------------------------------------------------------------
// Script execution and verification
// ---------------------------------------------------------------------------

type violation struct {
	at     int
	step   *step
	msg    string
	script []*step
}

// failureKey normalizes a failure message to the property that was violated,
// ignoring volatile values (ids, numbers, quoted values). It verifies that a
// shrink candidate reproduces the *same* divergence, not just any error.
func failureKey(msg string) string {
	line := msg
	if index := strings.IndexByte(line, '\n'); index >= 0 {
		line = line[:index]
	}
	tail := line
	for _, marker := range []string{"detail mismatch: ", "inconsistent with model: ", "): "} {
		if index := strings.Index(line, marker); index >= 0 {
			tail = line[index+len(marker):]
			break
		}
	}
	for _, sep := range []string{"=", `"`} {
		if index := strings.Index(tail, sep); index >= 0 {
			tail = tail[:index]
		}
	}
	var b strings.Builder
	for _, r := range tail {
		if r >= '0' && r <= '9' {
			continue
		}
		b.WriteRune(r)
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

func executeScript(t *testing.T, planned []*step) (*world, *violation) {
	t.Helper()
	e := newEnv()
	defer e.close()
	w := newWorld()

	deviceIDs := map[int]string{}
	caseIDs := map[int]string{}

	for i, plannedStep := range planned {
		s := plannedStep.deepCopy()
		method, path, body := s.request()
		result := e.doScript(t, method, path, body, deviceIDs, caseIDs)
		code := errorCode(result.raw)

		if result.status != s.wantStatus || (s.wantCode != "" && code != s.wantCode) {
			return w, &violation{
				at:     i,
				step:   s,
				script: planned,
				msg: fmt.Sprintf("step %d (%s): response status=%d code=%q, want status=%d code=%q; body=%s",
					i, s.note, result.status, code, s.wantStatus, s.wantCode, strings.TrimSpace(string(result.raw))),
			}
		}

		// Identify the concrete case path for before/after reads. For a step
		// targeting a known case that is the readback target.
		targetPath := ""
		if s.caseSym >= 0 {
			targetPath = "/cases/" + caseIDs[s.caseSym]
		}

		var preDetail, postDetail *contracts.CaseResponse
		var preEvents, postEvents *contracts.EventsResponse
		var preStatus, postStatus int
		if targetPath != "" {
			preStatus, preDetail, preEvents = readCaseViews(t, e, targetPath)
		}

		// Capture ids of newly created resources before advancing the model.
		switch s.kind {
		case opRegister:
			if s.predictedSuccess() {
				var view contracts.DeviceResponse
				mustDecode(t, result.raw, &view)
				deviceIDs[s.newDeviceSym] = view.Device.ID
			}
		case opOpen:
			if s.predictedSuccess() {
				var view contracts.CaseResponse
				mustDecode(t, result.raw, &view)
				caseIDs[s.newCaseSym] = view.Case.ID
			}
		}

		w.apply(s)

		// The mutation response itself must be consistent (no half-write that
		// is only visible in the committing response).
		if s.predictedSuccess() {
			switch s.kind {
			case opRegister:
				var view contracts.DeviceResponse
				mustDecode(t, result.raw, &view)
				d := w.devices[s.newDeviceSym]
				if view.Device.ID != d.id || view.Device.AssetTag != d.tag || string(view.Device.Status) != statusActive {
					return w, &violation{at: i, step: s, script: planned,
						msg: fmt.Sprintf("step %d (%s): register response id=%q tag=%q status=%q, want id=%q tag=%q",
							i, s.note, view.Device.ID, view.Device.AssetTag, view.Device.Status, d.id, d.tag)}
				}
			case opOpen, opMeasure, opReopen, opSeal:
				var view contracts.CaseResponse
				mustDecode(t, result.raw, &view)
				c := w.cases[caseSymOf(s)]
				if msg := compareCaseView(c, w.devices[c.deviceSym].id, &view); msg != "" {
					return w, &violation{at: i, step: s, script: planned,
						msg: fmt.Sprintf("step %d (%s): mutation response inconsistent with model: %s", i, s.note, msg)}
				}
			}
		}

		if targetPath != "" {
			postStatus, postDetail, postEvents = readCaseViews(t, e, targetPath)
		}

		// Rejected/rolled-back mutation: detail and event reads must be
		// identical to the committed state observed before the request.
		if !s.predictedSuccess() && targetPath != "" {
			if preStatus != postStatus {
				return w, &violation{at: i, step: s, script: planned,
					msg: fmt.Sprintf("step %d (%s): rejected mutation changed GET %s status %d -> %d",
						i, s.note, targetPath, preStatus, postStatus)}
			}
			if preStatus == http.StatusOK {
				if !reflect.DeepEqual(preDetail, postDetail) {
					return w, &violation{at: i, step: s, script: planned,
						msg: fmt.Sprintf("step %d (%s): rejected mutation left a half-written case view:\nbefore=%s\nafter =%s",
							i, s.note, mustJSON(preDetail), mustJSON(postDetail))}
				}
				if !reflect.DeepEqual(preEvents, postEvents) {
					return w, &violation{at: i, step: s, script: planned,
						msg: fmt.Sprintf("step %d (%s): rejected mutation left a half-written event view:\nbefore=%s\nafter =%s",
							i, s.note, mustJSON(preEvents), mustJSON(postEvents))}
				}
			}
		}

		if msg := w.verifyAll(t, e, deviceIDs, caseIDs); msg != "" {
			return w, &violation{at: i, step: s, script: planned,
				msg: fmt.Sprintf("step %d (%s): %s", i, s.note, msg)}
		}
	}
	return w, nil
}

func caseSymOf(s *step) int {
	if s.newCaseSym >= 0 {
		return s.newCaseSym
	}
	return s.caseSym
}

func readCaseViews(t *testing.T, e *env, casePath string) (int, *contracts.CaseResponse, *contracts.EventsResponse) {
	t.Helper()
	detailResult := e.do(t, http.MethodGet, casePath, nil)
	var detail *contracts.CaseResponse
	if detailResult.status == http.StatusOK {
		var view contracts.CaseResponse
		mustDecode(t, detailResult.raw, &view)
		detail = &view
	}
	eventsResult := e.do(t, http.MethodGet, casePath+"/events", nil)
	var events *contracts.EventsResponse
	if eventsResult.status == http.StatusOK {
		var view contracts.EventsResponse
		mustDecode(t, eventsResult.raw, &view)
		events = &view
	}
	return detailResult.status, detail, events
}

// verifyAll reads every modeled resource through the public API and compares it
// with the specification model, including the at-most-one-unfinished-case rule.
func (w *world) verifyAll(t *testing.T, e *env, deviceIDs, caseIDs map[int]string) string {
	t.Helper()

	for _, sym := range w.deviceOrder {
		count := 0
		for _, csym := range w.caseOrder {
			c := w.cases[csym]
			if c.deviceSym == sym && c.state != stateSealed {
				count++
			}
		}
		if count > 1 {
			return fmt.Sprintf("model invariant violated: device %s has %d unfinished cases", deviceIDs[sym], count)
		}
	}

	for _, sym := range w.deviceOrder {
		result := e.do(t, http.MethodGet, "/devices/"+deviceIDs[sym], nil)
		if result.status != http.StatusOK {
			return fmt.Sprintf("GET device %s status=%d body=%s", deviceIDs[sym], result.status, result.raw)
		}
		var view contracts.DeviceResponse
		mustDecode(t, result.raw, &view)
		d := w.devices[sym]
		if view.Device.ID != d.id || view.Device.AssetTag != d.tag || string(view.Device.Status) != statusActive {
			return fmt.Sprintf("device view mismatch:\nmodel=%s\nview =%s", mustJSON(d), mustJSON(view.Device))
		}
	}

	for _, sym := range w.caseOrder {
		c := w.cases[sym]
		id := caseIDs[sym]

		detailResult := e.do(t, http.MethodGet, "/cases/"+id, nil)
		if detailResult.status != http.StatusOK {
			return fmt.Sprintf("GET case %s status=%d body=%s", id, detailResult.status, detailResult.raw)
		}
		var detail contracts.CaseResponse
		mustDecode(t, detailResult.raw, &detail)
		if msg := compareCaseView(c, w.devices[c.deviceSym].id, &detail); msg != "" {
			return fmt.Sprintf("case %s detail mismatch: %s\nmodel=%s\nview =%s", id, msg, mustJSON(c), mustJSON(detail))
		}

		eventsResult := e.do(t, http.MethodGet, "/cases/"+id+"/events", nil)
		if eventsResult.status != http.StatusOK {
			return fmt.Sprintf("GET case %s events status=%d", id, eventsResult.status)
		}
		var eventsView contracts.EventsResponse
		mustDecode(t, eventsResult.raw, &eventsView)
		if !reflect.DeepEqual(eventsView.Events, detail.Events) {
			return fmt.Sprintf("case %s events endpoint does not match detail events:\nevents=%s\ndetail=%s",
				id, mustJSON(eventsView.Events), mustJSON(detail.Events))
		}
		// Events must never leak across cases: every returned event links here.
		for _, ev := range eventsView.Events {
			if ev.CaseID != id || ev.SubjectType != "case" || ev.SubjectID != id {
				return fmt.Sprintf("case %s returned a foreign event: %s", id, mustJSON(ev))
			}
		}
	}
	return ""
}

// compareCaseView returns "" when the view exactly matches the model.
func compareCaseView(c *modelCase, wantDeviceID string, view *contracts.CaseResponse) string {
	if view.Case.ID != c.id {
		return fmt.Sprintf("case id=%q want %q", view.Case.ID, c.id)
	}
	if view.Case.DeviceID != wantDeviceID {
		return fmt.Sprintf("case %s device_id=%q want %q", c.id, view.Case.DeviceID, wantDeviceID)
	}
	if got := string(view.Case.State); got != c.state {
		return fmt.Sprintf("case %s state=%q want %q", c.id, got, c.state)
	}
	if view.Case.Version != c.version {
		return fmt.Sprintf("case %s version=%d want %d", c.id, view.Case.Version, c.version)
	}
	if view.Case.Nominal != c.nominal {
		return fmt.Sprintf("case %s nominal=%v want %v", c.id, view.Case.Nominal, c.nominal)
	}
	if view.Case.Tolerance != c.tolerance {
		return fmt.Sprintf("case %s tolerance=%v want %v", c.id, view.Case.Tolerance, c.tolerance)
	}
	if view.Case.Steward != c.steward {
		return fmt.Sprintf("case %s steward=%q want %q", c.id, view.Case.Steward, c.steward)
	}
	if (view.Case.SealedAt != nil) != (c.state == stateSealed) {
		return fmt.Sprintf("case %s sealed_at presence mismatch", c.id)
	}

	if len(view.Measurements) != len(c.measurements) {
		return fmt.Sprintf("case %s measurements len=%d want %d", c.id, len(view.Measurements), len(c.measurements))
	}
	for i, m := range c.measurements {
		got := view.Measurements[i]
		if got.ID != m.id {
			return fmt.Sprintf("case %s measurement[%d] id=%q want %q", c.id, i, got.ID, m.id)
		}
		if got.Observed != m.observed {
			return fmt.Sprintf("case %s measurement[%d] observed=%v want %v", c.id, i, got.Observed, m.observed)
		}
		if got.Accepted != m.accepted {
			return fmt.Sprintf("case %s measurement[%d] accepted=%v want %v", c.id, i, got.Accepted, m.accepted)
		}
		if got.Technician != m.technician {
			return fmt.Sprintf("case %s measurement[%d] technician=%q want %q", c.id, i, got.Technician, m.technician)
		}
		if got.CaseID != c.id || got.Nominal != c.nominal || got.Tolerance != c.tolerance {
			return fmt.Sprintf("case %s measurement[%d] header fields wrong: %s", c.id, i, mustJSON(got))
		}
		if got.Deviation != m.observed-c.nominal {
			return fmt.Sprintf("case %s measurement[%d] deviation=%v want %v", c.id, i, got.Deviation, m.observed-c.nominal)
		}
	}

	hasSeal := view.Seal != nil
	wantSeal := c.seal != nil
	if hasSeal != wantSeal {
		return fmt.Sprintf("case %s seal presence=%v want %v", c.id, hasSeal, wantSeal)
	}
	if wantSeal {
		if view.Seal.ID != c.seal.id {
			return fmt.Sprintf("case %s seal id=%q want %q", c.id, view.Seal.ID, c.seal.id)
		}
		if view.Seal.MeasurementID != c.seal.measurementID {
			return fmt.Sprintf("case %s seal measurement=%q want %q", c.id, view.Seal.MeasurementID, c.seal.measurementID)
		}
		if view.Seal.Reviewer != c.seal.reviewer {
			return fmt.Sprintf("case %s seal reviewer=%q want %q", c.id, view.Seal.Reviewer, c.seal.reviewer)
		}
		if view.Seal.CaseID != c.id {
			return fmt.Sprintf("case %s seal case id=%q", c.id, view.Seal.CaseID)
		}
		minDev := math.MaxFloat64
		var sealedAccepted bool
		for _, m := range view.Measurements {
			if m.ID == view.Seal.MeasurementID {
				if !m.Accepted {
					return fmt.Sprintf("case %s seal points at rejected measurement %s", c.id, m.ID)
				}
				if dev := math.Abs(m.Observed - c.nominal); dev < minDev {
					minDev = dev
				}
				sealedAccepted = true
			}
		}
		if !sealedAccepted {
			return fmt.Sprintf("case %s seal points to measurement %s not in history", c.id, view.Seal.MeasurementID)
		}
		for _, m := range view.Measurements {
			if m.Accepted && math.Abs(m.Observed-c.nominal) < minDev {
				return fmt.Sprintf("case %s seal did not pick minimal-deviation accepted measurement", c.id)
			}
		}
	}

	if len(view.Events) != len(c.events) {
		return fmt.Sprintf("case %s events len=%d want %d: detail=%s", c.id, len(view.Events), len(c.events), mustJSON(view.Events))
	}
	for i, ev := range c.events {
		got := view.Events[i]
		if got.ID != ev.id {
			return fmt.Sprintf("case %s event[%d] id=%q want %q", c.id, i, got.ID, ev.id)
		}
		if got.Kind != ev.kind {
			return fmt.Sprintf("case %s event[%d] kind=%q want %q", c.id, i, got.Kind, ev.kind)
		}
		if got.Actor != ev.actor {
			return fmt.Sprintf("case %s event[%d] actor=%q want %q", c.id, i, got.Actor, ev.actor)
		}
		if got.CaseID != c.id || got.SubjectType != "case" || got.SubjectID != c.id {
			return fmt.Sprintf("case %s event[%d] linkage wrong: %s", c.id, i, mustJSON(got))
		}
		if got.Detail == "" {
			return fmt.Sprintf("case %s event[%d] detail is empty", c.id, i)
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Shrinking: reduce a failing script to a minimal reproducing prefix
// ---------------------------------------------------------------------------

func copyScript(script []*step) []*step {
	out := make([]*step, len(script))
	for i, s := range script {
		out[i] = s.deepCopy()
	}
	return out
}

// shrinkScript greedily removes steps (and their dependent resources) while the
// same divergence still reproduces, returning a minimal concrete transcript.
func shrinkScript(t *testing.T, planned []*step, v *violation) []*step {
	t.Helper()
	wantKey := failureKey(v.msg)
	current := copyScript(planned[:v.at+1])
	for pass := 0; pass < 6; pass++ {
		changed := false
		// Keep the last step (the failing one); try removing every other step.
		for i := len(current) - 2; i >= 0; i-- {
			candidate := removeStepWithClosure(current, i)
			if candidate == nil {
				continue
			}
			_, failure := executeScript(t, candidate)
			if failure != nil && failureKey(failure.msg) == wantKey {
				current = candidate
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	return current
}

// removeStepWithClosure removes step i and every step that depends on a
// resource it creates. Returns nil if removal would also remove the last
// (failing) step.
func removeStepWithClosure(script []*step, index int) []*step {
	removedDevices := map[int]bool{}
	removedCases := map[int]bool{}

	if s := script[index]; s.predictedSuccess() {
		if s.kind == opRegister {
			removedDevices[s.newDeviceSym] = true
		}
		if s.kind == opOpen {
			removedCases[s.newCaseSym] = true
		}
	}

	removed := map[int]bool{index: true}
	for growth := true; growth; {
		growth = false
		for i, s := range script {
			if i == len(script)-1 {
				if removedDevices[s.deviceSym] || removedCases[s.caseSym] ||
					removedDevices[s.newDeviceSym] || removedCases[s.newCaseSym] {
					return nil
				}
				continue
			}
			if removed[i] {
				continue
			}
			depends := removedDevices[s.deviceSym] || removedCases[s.caseSym]
			if !depends && s.predictedSuccess() && s.kind == opOpen && removedDevices[s.deviceSym] {
				depends = true
			}
			if depends {
				removed[i] = true
				growth = true
				if s.predictedSuccess() && s.kind == opOpen {
					removedCases[s.newCaseSym] = true
				}
				if s.predictedSuccess() && s.kind == opRegister {
					removedDevices[s.newDeviceSym] = true
				}
			}
		}
	}

	out := make([]*step, 0, len(script)-1)
	for i, s := range script {
		if !removed[i] {
			out = append(out, s.deepCopy())
		}
	}
	return out
}

// renderExecutedScript runs the script against a fresh server and renders the
// concrete HTTP transcript, including the last response body.
func renderExecutedScript(t *testing.T, seed int64, planned []*step) string {
	t.Helper()
	e := newEnv()
	defer e.close()
	deviceIDs := map[int]string{}
	caseIDs := map[int]string{}

	var b strings.Builder
	fmt.Fprintf(&b, "seed=%d, %d requests:\n", seed, len(planned))
	for i, plannedStep := range planned {
		s := plannedStep.deepCopy()
		method, path, body := s.request()
		displayPath := resolveTokens(path, deviceIDs, caseIDs)
		displayBody := ""
		if body != nil {
			displayBody = resolveTokens(string(mustJSON(body)), deviceIDs, caseIDs)
		}
		result := e.doScript(t, method, path, body, deviceIDs, caseIDs)
		code := errorCode(result.raw)

		switch s.kind {
		case opRegister:
			if result.status == http.StatusCreated {
				var view contracts.DeviceResponse
				mustDecode(t, result.raw, &view)
				deviceIDs[s.newDeviceSym] = view.Device.ID
			}
		case opOpen:
			if result.status == http.StatusCreated {
				var view contracts.CaseResponse
				mustDecode(t, result.raw, &view)
				caseIDs[s.newCaseSym] = view.Case.ID
			}
		}

		fmt.Fprintf(&b, "%3d. %s %s %s -> %d %s  (%s)\n", i+1, method, displayPath, displayBody, result.status, code, s.note)
		if i == len(planned)-1 {
			fmt.Fprintf(&b, "     response: %s\n", strings.TrimSpace(string(result.raw)))
		}
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Small shared helpers
// ---------------------------------------------------------------------------

func errorCode(raw []byte) string {
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return ""
	}
	return envelope.Error.Code
}

func mustDecode(t *testing.T, raw []byte, target any) {
	t.Helper()
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("decode response %s: %v", string(raw), err)
	}
}

func mustJSON(value any) []byte {
	buf, err := json.Marshal(value)
	if err != nil {
		return []byte(fmt.Sprintf("<marshal error: %v>", err))
	}
	return buf
}

// nextSeqID maps a prefix-NNNN id to the following one.
func nextSeqID(id string) string {
	index := strings.LastIndex(id, "-")
	if index < 0 {
		return ""
	}
	n, err := strconv.Atoi(id[index+1:])
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%s-%04d", id[:index], n+1)
}
