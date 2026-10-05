package statetest_test

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"example.com/solo-0021-metrology-assurance-suite/internal/domain"
)

// States, transition kinds and stable error codes are taken from the public
// specification (PROJECT_SPEC.md "State And Rules" and the error envelope in
// README.md).
const (
	stAwaiting = string(domain.CaseAwaitingMeasurement)
	stReview   = string(domain.CaseReviewReady)
	stRecheck  = string(domain.CaseRecheckRequired)
	stSealed   = string(domain.CaseSealed)

	stDeviceActive  = string(domain.DeviceActive)
	stDeviceRetired = string(domain.DeviceRetired)

	evDeviceEnrolled = "device_enrolled"
	evCaseOpened     = "case_opened"
	evMeasurementOK  = "measurement_accepted"
	evMeasurementBad = "measurement_rejected"
	evCaseReopened   = "case_reopened"
	evCaseSealed     = "case_sealed"

	codeInvalid    = "invalid_input"
	codeNotFound   = "not_found"
	codeDuplicate  = "duplicate_asset_tag"
	codeActiveCase = "active_case_exists"
	codeState      = "state_conflict"
	codeSealExists = "seal_exists"
	codeInternal   = "internal_error"
)

// modelCase is the oracle's bookkeeping for one case. It stores exactly the
// facts the API exposes; hidden production state would make the model unable to
// predict responses and is itself a finding.
type modelCase struct {
	detail caseDetail
	// nextEventCount is the number of case events the oracle predicts after a
	// successful mutation; the version must move in lockstep with state-change
	// events.
}

type modelDevice struct {
	device viewDevice
}

// model is an independent reimplementation of the documented state machine.
// Counters reproduce the per-prefix identifier scheme so every successful
// response can be required to carry the exact identifier the oracle predicted;
// a failed mutation must leave every counter untouched (no ghost ids).
type model struct {
	devices  map[string]*modelDevice
	byTag    map[string]string // normalized tag -> device id
	cases    map[string]*modelCase
	counters map[string]int
	// eventOrder is the predicted global append order of all events, keyed by
	// event id. Commit order across concurrent mutations is derived from the
	// observed event ids and replayed on a copy of the model.
	eventOrder []string
}

func newModel() *model {
	return &model{
		devices:  map[string]*modelDevice{},
		byTag:    map[string]string{},
		cases:    map[string]*modelCase{},
		counters: map[string]int{},
	}
}

// deepCopy returns an isolated copy used to explore alternative serializations
// of a concurrent batch. Slices and pointers are duplicated so mutations to
// the copy cannot alias the original.
func (m *model) deepCopy() *model {
	cp := &model{
		devices:  make(map[string]*modelDevice, len(m.devices)),
		byTag:    make(map[string]string, len(m.byTag)),
		cases:    make(map[string]*modelCase, len(m.cases)),
		counters: make(map[string]int, len(m.counters)),
	}
	for k, v := range m.counters {
		cp.counters[k] = v
	}
	for k, v := range m.byTag {
		cp.byTag[k] = v
	}
	for id, dev := range m.devices {
		deviceCopy := *dev
		cp.devices[id] = &deviceCopy
	}
	for id, c := range m.cases {
		cp.cases[id] = &modelCase{detail: copyDetail(c.detail)}
	}
	cp.eventOrder = append(cp.eventOrder, m.eventOrder...)
	return cp
}

// copyDetail duplicates every slice/pointer inside a case detail.
func copyDetail(in caseDetail) caseDetail {
	out := in
	out.Measurements = append([]viewMeasurement(nil), in.Measurements...)
	out.Events = append([]viewEvent(nil), in.Events...)
	if in.Seal != nil {
		seal := *in.Seal
		out.Seal = &seal
	}
	return out
}

func (m *model) nextID(prefix string) string {
	m.counters[prefix]++
	return fmt.Sprintf("%s-%04d", prefix, m.counters[prefix])
}

func (m *model) peekID(prefix string) string {
	return fmt.Sprintf("%s-%04d", prefix, m.counters[prefix]+1)
}

func normalizeTag(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

// outcome predicts one attempt. status==0 marks a malformed request whose
// status is predicted directly by the caller (always 400).
type outcome struct {
	ok         bool
	status     int
	code       string
	caseIDHint string // case the attempt targeted, for coherent reads
}

// registerPredict applies a register attempt to the oracle. It returns the
// predicted outcome and, on success, the device id the response must carry.
func (m *model) registerPredict(tag, modelName, room, steward string) (outcome, string) {
	normalized := normalizeTag(tag)
	valid := normalized != "" && strings.TrimSpace(modelName) != "" &&
		strings.TrimSpace(room) != "" && strings.TrimSpace(steward) != "" && len(normalized) <= 64
	if !valid {
		return outcome{status: statusBad, code: codeInvalid}, ""
	}
	if _, taken := m.byTag[normalized]; taken {
		return outcome{status: statusConflict, code: codeDuplicate}, ""
	}
	id := m.nextID("device")
	eventID := m.nextID("event")
	now := modelZero
	m.devices[id] = &modelDevice{device: viewDevice{
		ID:         id,
		AssetTag:   normalized,
		Model:      strings.TrimSpace(modelName),
		Room:       strings.TrimSpace(room),
		Steward:    strings.TrimSpace(steward),
		Status:     stDeviceActive,
		EnrolledAt: now,
	}}
	m.byTag[normalized] = id
	m.eventOrder = append(m.eventOrder, eventID)
	return outcome{ok: true, status: statusCreated}, id
}

// openPredict applies an open-case attempt.
func (m *model) openPredict(deviceID string, nominal, tolerance float64, steward string) (outcome, string) {
	deviceID = strings.TrimSpace(deviceID)
	steward = strings.TrimSpace(steward)
	device, exists := m.devices[deviceID]
	// Validation order reproduced from the service boundary: existence and
	// lifecycle are rejected with the documented state/conflict codes before
	// body validation would matter; invalid bodies (non-positive tolerance)
	// arrive with an existing active device in the generated sequences.
	if !exists {
		return outcome{status: statusNotFound, code: codeNotFound}, ""
	}
	if device.device.Status != stDeviceActive {
		return outcome{status: statusConflict, code: codeState}, ""
	}
	// Production checks the unfinished-case guard before constructing the new
	// aggregate, so an active case wins over an invalid tolerance in the error
	// mapping; the model reproduces that precedence.
	if other, busy := m.unfinishedCase(deviceID); busy {
		return outcome{status: statusConflict, code: codeActiveCase, caseIDHint: other}, ""
	}
	if tolerance <= 0 || steward == "" {
		return outcome{status: statusBad, code: codeInvalid}, ""
	}
	id := m.nextID("case")
	eventID := m.nextID("event")
	now := modelZero
	opened := &modelCase{detail: caseDetail{
		Case: viewCase{
			ID:        id,
			DeviceID:  deviceID,
			Nominal:   nominal,
			Tolerance: tolerance,
			Steward:   steward,
			State:     stAwaiting,
			Version:   1,
			OpenedAt:  now,
			UpdatedAt: now,
		},
		Measurements: nil, // production serializes an empty slice as JSON null
		Events: []viewEvent{{
			ID:          eventID,
			SubjectType: "case",
			SubjectID:   id,
			CaseID:      id,
			Kind:        evCaseOpened,
			Actor:       steward,
			Detail:      "verification case opened",
			CreatedAt:   now,
		}},
	}}
	m.cases[id] = opened
	m.eventOrder = append(m.eventOrder, eventID)
	return outcome{ok: true, status: statusCreated, caseIDHint: id}, id
}

// measurementPredict applies a record-measurement attempt. The observed value
// decides acceptance using the documented inclusive tolerance rule.
func (m *model) measurementPredict(caseID string, observed float64, technician string) outcome {
	c := m.cases[strings.TrimSpace(caseID)]
	if c == nil {
		return outcome{status: statusNotFound, code: codeNotFound, caseIDHint: caseID}
	}
	if strings.TrimSpace(technician) == "" {
		// Observed production ordering validates the constructed measurement
		// before the state guard; either way the mutation is rejected and no
		// fact may be left behind.
		return outcome{status: statusBad, code: codeInvalid, caseIDHint: caseID}
	}
	if c.detail.Case.State != stAwaiting {
		return outcome{status: statusConflict, code: codeState, caseIDHint: caseID}
	}
	measurementID := m.nextID("measurement")
	eventID := m.nextID("event")
	deviation := observed - c.detail.Case.Nominal
	accepted := absFloat(deviation) <= c.detail.Case.Tolerance
	now := modelZero
	kind := evMeasurementBad
	state := stRecheck
	detail := "measurement exceeded tolerance and requires another measurement"
	if accepted {
		kind = evMeasurementOK
		state = stReview
		detail = "measurement is within tolerance and ready for review"
	}
	c.detail.Measurements = append(c.detail.Measurements, viewMeasurement{
		ID:         measurementID,
		CaseID:     c.detail.Case.ID,
		Observed:   observed,
		Nominal:    c.detail.Case.Nominal,
		Deviation:  deviation,
		Tolerance:  c.detail.Case.Tolerance,
		Accepted:   accepted,
		Technician: strings.TrimSpace(technician),
		CreatedAt:  now,
	})
	c.detail.Events = append(c.detail.Events, viewEvent{
		ID:          eventID,
		SubjectType: "case",
		SubjectID:   c.detail.Case.ID,
		CaseID:      c.detail.Case.ID,
		Kind:        kind,
		Actor:       strings.TrimSpace(technician),
		Detail:      detail,
		CreatedAt:   now,
	})
	c.detail.Case.State = state
	c.detail.Case.Version++
	c.detail.Case.UpdatedAt = now
	m.eventOrder = append(m.eventOrder, eventID)
	return outcome{ok: true, status: statusCreated, caseIDHint: caseID}
}

// reopenPredict applies a reopen attempt.
func (m *model) reopenPredict(caseID, actor, reason string) outcome {
	c := m.cases[strings.TrimSpace(caseID)]
	if c == nil {
		return outcome{status: statusNotFound, code: codeNotFound, caseIDHint: caseID}
	}
	actor = strings.TrimSpace(actor)
	reason = strings.TrimSpace(reason)
	if actor == "" || reason == "" {
		// Spec rule "Only recheck_required cases may move back" is a state
		// rule; an empty actor is an invalid body. The oracle treats the
		// state machine as authoritative when the case cannot move, matching
		// the production guard ordering; the rollback probes below assert the
		// spec invariant (no partial change) regardless of which 4xx wins.
		if c.detail.Case.State != stRecheck {
			return outcome{status: statusConflict, code: codeState, caseIDHint: caseID}
		}
		return outcome{status: statusBad, code: codeInvalid, caseIDHint: caseID}
	}
	if c.detail.Case.State != stRecheck {
		return outcome{status: statusConflict, code: codeState, caseIDHint: caseID}
	}
	eventID := m.nextID("event")
	now := modelZero
	c.detail.Events = append(c.detail.Events, viewEvent{
		ID:          eventID,
		SubjectType: "case",
		SubjectID:   c.detail.Case.ID,
		CaseID:      c.detail.Case.ID,
		Kind:        evCaseReopened,
		Actor:       actor,
		Detail:      reason,
		CreatedAt:   now,
	})
	c.detail.Case.State = stAwaiting
	c.detail.Case.Version++
	c.detail.Case.UpdatedAt = now
	m.eventOrder = append(m.eventOrder, eventID)
	return outcome{ok: true, status: statusOK, caseIDHint: caseID}
}

// sealPredict applies a seal attempt. The seal must select the accepted
// measurement with the smallest absolute deviation.
func (m *model) sealPredict(caseID, reviewer, summary string) outcome {
	c := m.cases[strings.TrimSpace(caseID)]
	if c == nil {
		return outcome{status: statusNotFound, code: codeNotFound, caseIDHint: caseID}
	}
	reviewer = strings.TrimSpace(reviewer)
	summary = strings.TrimSpace(summary)
	if c.detail.Seal != nil {
		return outcome{status: statusConflict, code: codeSealExists, caseIDHint: caseID}
	}
	if reviewer == "" || summary == "" {
		if c.detail.Case.State != stReview {
			return outcome{status: statusConflict, code: codeState, caseIDHint: caseID}
		}
		return outcome{status: statusBad, code: codeInvalid, caseIDHint: caseID}
	}
	if c.detail.Case.State != stReview {
		return outcome{status: statusConflict, code: codeState, caseIDHint: caseID}
	}
	best, ok := bestAccepted(c.detail.Measurements)
	if !ok {
		return outcome{status: statusConflict, code: codeState, caseIDHint: caseID}
	}
	sealID := m.nextID("seal")
	eventID := m.nextID("event")
	now := modelZero
	c.detail.Seal = &viewSeal{
		ID:            sealID,
		CaseID:        c.detail.Case.ID,
		DeviceID:      c.detail.Case.DeviceID,
		MeasurementID: best.ID,
		Reviewer:      reviewer,
		Summary:       summary,
		SealedAt:      now,
	}
	c.detail.Events = append(c.detail.Events, viewEvent{
		ID:          eventID,
		SubjectType: "case",
		SubjectID:   c.detail.Case.ID,
		CaseID:      c.detail.Case.ID,
		Kind:        evCaseSealed,
		Actor:       reviewer,
		Detail:      summary,
		CreatedAt:   now,
	})
	c.detail.Case.State = stSealed
	c.detail.Case.Version++
	c.detail.Case.UpdatedAt = now
	c.detail.Case.SealedAt = now
	m.eventOrder = append(m.eventOrder, eventID)
	return outcome{ok: true, status: statusCreated, caseIDHint: caseID}
}

func (m *model) unfinishedCase(deviceID string) (string, bool) {
	for id, c := range m.cases {
		if c.detail.Case.DeviceID == deviceID && c.detail.Case.State != stSealed {
			return id, true
		}
	}
	return "", false
}

// Timestamps are not predicted: the service stamps every fact with time.Now,
// so the oracle leaves them zero and reconciliation compares all non-time
// fields exactly while separate invariant checks assert the ordering and
// equality relations among the observed timestamps.
var modelZero = time.Time{}

func bestAccepted(items []viewMeasurement) (viewMeasurement, bool) {
	index := -1
	for i, item := range items {
		if !item.Accepted {
			continue
		}
		if index == -1 || absFloat(item.Deviation) < absFloat(items[index].Deviation) {
			index = i
		}
	}
	if index == -1 {
		return viewMeasurement{}, false
	}
	return items[index], true
}

func absFloat(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

// sortedCaseIDs returns case ids in a stable order for full reconciliation.
func sortedCaseIDs(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

func (m *model) caseIDs() []string {
	ids := make([]string, 0, len(m.cases))
	for id := range m.cases {
		ids = append(ids, id)
	}
	return sortedCaseIDs(ids)
}

func (m *model) deviceIDs() []string {
	ids := make([]string, 0, len(m.devices))
	for id := range m.devices {
		ids = append(ids, id)
	}
	return sortedCaseIDs(ids)
}
