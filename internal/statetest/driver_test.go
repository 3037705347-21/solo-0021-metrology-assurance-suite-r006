package statetest_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// mustDo performs an HTTP call from the sequential driver, where calling
// t.Fatalf on a transport/decode failure is appropriate.
func (d *driver) mustDo(method, path string, body any, raw ...[]byte) rpcResult {
	d.t.Helper()
	res, err := d.h.do(method, path, body, raw...)
	if err != nil {
		d.t.Fatalf("%s %s: %v", method, path, err)
	}
	return res
}

// opKind enumerates the generated sequential operations. Reads are generated
// too: after every rejected or rolled-back mutation the affected case is
// re-read and reconciled against the oracle, and a periodic full sweep reads
// every device and case.
type opKind int

const (
	opRegister opKind = iota
	opRegisterDuplicate
	opRegisterInvalid
	opOpen
	opOpenUnknownDevice
	opOpenInvalidTolerance
	opMeasure
	opMeasureBoundary
	opMeasureEmptyTechnician
	opReopen
	opReopenEmptyActor
	opSeal
	opSealEmptyReviewer
	opMalformedJSON
)

// driver carries the deterministic RNG, oracle, HTTP harness and running
// statistics for one sequential scenario.
type driver struct {
	t    *testing.T
	h    *harness
	m    *model
	rng  *rand.Rand
	seed int64
	step int

	// fixed catalogue of people so payloads stay well formed unless an
	// operation deliberately corrupts them.
	people []string
	rooms  []string

	stats map[string]int

	// checkCommitClock enables the strict same-commit timestamp assertion.
	// The determinism rerun only compares clock-independent facts, so it flips
	// this off; every other scenario leaves it on.
	checkCommitClock bool

	// Per-step randomness pool. Exactly the same number of values is drawn at
	// the start of every step regardless of the model state or the live
	// response, so a fixed seed reproduces the identical operation sequence.
	// Executors consume from the pool through rint/rfloat and never touch rng
	// themselves; otherwise a rejected request that skipped a draw would
	// desync every later step.
	pool    []int
	poolF   float64
	poolPos int
}

const poolIntsPerStep = 16
const poolIntRange = 1_000_000

// rint returns a deterministic value in [0,n).
func (d *driver) rint(n int) int {
	if n <= 0 {
		return 0
	}
	if d.poolPos >= len(d.pool) {
		d.t.Fatalf("random pool exhausted for step %d (need more than %d ints)", d.step, poolIntsPerStep)
	}
	value := d.pool[d.poolPos] % n
	d.poolPos++
	return value
}

func (d *driver) rfloat() float64 { return d.poolF }

func (d *driver) refillPool() {
	d.pool = make([]int, poolIntsPerStep)
	for i := range d.pool {
		d.pool[i] = d.rng.Intn(poolIntRange)
	}
	d.poolF = d.rng.Float64()
	d.poolPos = 0
}

func newDriver(t *testing.T, h *harness, seed int64) *driver {
	return &driver{
		t:                t,
		h:                h,
		m:                newModel(),
		rng:              rand.New(rand.NewSource(seed)),
		seed:             seed,
		people:           []string{"Lin", "Mira", "Qiao", "Rui", "Anke", "Bram"},
		rooms:            []string{"Room A", "Room B", "Bench 3", "Cell 7"},
		stats:            map[string]int{},
		checkCommitClock: true,
	}
}

func (d *driver) count(name string) { d.stats[name]++ }

// pickCase returns a random known case id from the step pool.
func (d *driver) pickCase() (string, *modelCase) {
	ids := d.m.caseIDs()
	id := ids[d.rint(len(ids))]
	return id, d.m.cases[id]
}

// pickOpenDevice returns an active device with no unfinished case, if one
// exists, so open attempts have a real chance of progressing the workflow.
func (d *driver) pickOpenDevice() (string, bool) {
	var candidates []string
	for _, id := range d.m.deviceIDs() {
		if d.m.devices[id].device.Status != stDeviceActive {
			continue
		}
		if _, busy := d.m.unfinishedCase(id); busy {
			continue
		}
		candidates = append(candidates, id)
	}
	if len(candidates) == 0 {
		return "", false
	}
	return candidates[d.rint(len(candidates))], true
}

// runSequential generates and verifies a fixed-length operation history.
func (d *driver) runSequential(devices, ops int) {
	d.t.Helper()

	// Seed phase: register several devices up front, including a normalization
	// collision, so later steps always have a multi-device population.
	d.refillPool()
	d.seedDevices(devices)

	for i := 0; i < ops; i++ {
		d.step = i + 1
		d.oneStep()
		if (i+1)%25 == 0 {
			d.reconcileEverything(fmt.Sprintf("seed=%d step=%d periodic", d.seed, d.step))
		}
	}
	d.reconcileEverything(fmt.Sprintf("seed=%d final", d.seed))
}

func (d *driver) seedDevices(n int) {
	for i := 0; i < n; i++ {
		tag := fmt.Sprintf("MET-S%02d-D%02d", d.seed%100, i)
		// Occasionally add surrounding whitespace / lowercase to exercise
		// normalization.
		if i%3 == 0 {
			tag = "  " + strings.ToLower(tag) + " "
		}
		d.registerOne(tag, false)
	}
	// One deliberate normalized-tag collision attempt.
	first := d.m.deviceIDs()[0]
	tag := d.m.devices[first].device.AssetTag
	d.registerOne(" "+strings.ToLower(tag)+"\t", true)
}

// oneStep chooses an operation biased toward what can still progress, while a
// fixed minority of steps deliberately hit rejection/rollback paths. All
// randomness is read from the pre-filled step pool.
func (d *driver) oneStep() {
	d.t.Helper()
	d.refillPool()

	// Build the progress set from a deterministic (sorted case id) walk so map
	// iteration order can never decide which operation the pool selects.
	hasKind := map[opKind]bool{}
	var progress []opKind
	addProgress := func(k opKind) {
		if !hasKind[k] {
			hasKind[k] = true
			progress = append(progress, k)
		}
	}
	for _, caseID := range d.m.caseIDs() {
		switch d.m.cases[caseID].detail.Case.State {
		case stAwaiting:
			addProgress(opMeasure)
		case stRecheck:
			addProgress(opReopen)
		case stReview:
			addProgress(opSeal)
		}
	}
	for _, id := range d.m.deviceIDs() {
		if d.m.devices[id].device.Status != stDeviceActive {
			continue
		}
		if _, busy := d.m.unfinishedCase(id); !busy {
			addProgress(opOpen)
			break
		}
	}

	roll := d.rint(100)
	var kind opKind
	switch {
	case roll < 45 && len(progress) > 0:
		kind = progress[d.rint(len(progress))]
	case roll < 60:
		// Random case-targeted op: often a terminal/wrong-state retry.
		kind = []opKind{opMeasure, opReopen, opSeal}[d.rint(3)]
	case roll < 68:
		kind = opRegister
	case roll < 72:
		kind = opRegisterDuplicate
	case roll < 75:
		kind = opRegisterInvalid
	case roll < 80:
		kind = opOpenUnknownDevice
	case roll < 84:
		kind = opOpenInvalidTolerance
	case roll < 88:
		kind = opMeasureBoundary
	case roll < 91:
		kind = opMeasureEmptyTechnician
	case roll < 94:
		kind = opReopenEmptyActor
	case roll < 97:
		kind = opSealEmptyReviewer
	default:
		kind = opMalformedJSON
	}
	if len(d.m.cases) == 0 && isCaseOp(kind) {
		kind = opOpen // no cases yet: open instead
	}
	d.count(fmt.Sprint(kind))
	d.dispatch(kind)
}

func isCaseOp(kind opKind) bool {
	switch kind {
	case opMeasure, opMeasureBoundary, opMeasureEmptyTechnician,
		opReopen, opReopenEmptyActor, opSeal, opSealEmptyReviewer,
		opMalformedJSON:
		return true
	}
	return false
}

func (d *driver) dispatch(kind opKind) {
	d.t.Helper()
	switch kind {
	case opRegister:
		tag := fmt.Sprintf("MET-S%02d-X%04d", d.seed%100, d.rint(9000)+1000)
		d.registerOne(tag, false)
	case opRegisterInvalid:
		// Empty asset tag after whitespace normalization -> invalid_input.
		d.registerOne("   ", false)
	case opRegisterDuplicate:
		id := d.m.deviceIDs()[d.rint(len(d.m.deviceIDs()))]
		tag := d.m.devices[id].device.AssetTag
		d.registerOne(" "+strings.ToLower(tag)+"\t", true)
	case opOpen:
		if id, ok := d.pickOpenDevice(); ok {
			d.openOne(id, 25.0, 0.5, "")
		} else if len(d.m.cases) > 0 {
			id, _ := d.pickCase()
			d.openOne(d.m.cases[id].detail.Case.DeviceID, 25.0, 0.5, "")
		} else {
			// Nothing openable: probe an unknown device rejection.
			d.openOne("device-9999", 25.0, 0.5, "")
		}
	case opOpenUnknownDevice:
		d.openOne("device-9999", 25.0, 0.5, "")
	case opOpenInvalidTolerance:
		// Target an active device WITHOUT an unfinished case so the request
		// reaches tolerance validation and returns invalid_input; if none
		// exists, probe the unknown-device rejection instead (still a 4xx with
		// no state change).
		id, ok := d.pickOpenDevice()
		if !ok {
			d.openOne("device-9999", 25.0, 0.5, "")
			return
		}
		tolerance := []float64{0, -0.5}[d.rint(2)]
		d.openOne(id, 25.0, tolerance, "")
	case opMeasure:
		id, c := d.pickCase()
		// Spread observed values across in/out of tolerance.
		span := 2.5 * c.detail.Case.Tolerance
		observed := c.detail.Case.Nominal + (d.rfloat()*2-1)*span
		d.measureOne(id, observed, d.people[d.rint(len(d.people))])
	case opMeasureBoundary:
		id, c := d.pickCase()
		// Exactly on the boundary must be accepted (inclusive rule).
		observed := c.detail.Case.Nominal + c.detail.Case.Tolerance
		if d.rint(2) == 0 {
			observed = c.detail.Case.Nominal - c.detail.Case.Tolerance
		}
		d.measureOne(id, observed, d.people[d.rint(len(d.people))])
	case opMeasureEmptyTechnician:
		id, c := d.pickCase()
		d.measureOne(id, c.detail.Case.Nominal, "")
	case opReopen:
		id, _ := d.pickCase()
		d.reopenOne(id, d.people[d.rint(len(d.people))], "measurement setup was replaced")
	case opReopenEmptyActor:
		id, _ := d.pickCase()
		d.reopenOne(id, "", "")
	case opSeal:
		id, _ := d.pickCase()
		d.sealOne(id, d.people[d.rint(len(d.people))], "accepted measurement sealed for review")
	case opSealEmptyReviewer:
		id, _ := d.pickCase()
		d.sealOne(id, "", "")
	case opMalformedJSON:
		id, _ := d.pickCase()
		choices := []struct {
			method string
			path   string
			body   string
		}{
			{"POST", "/devices", `{`},
			{"POST", "/cases/" + id + "/measurements", `{"observed":25.1,"technician":"Rui","bogus":1}`},
			{"POST", "/cases/" + id + "/seal", `not-json`},
			{"POST", "/cases/" + id + "/reopen", ``},
		}
		pick := choices[d.rint(len(choices))]
		d.rawOne(pick.method, pick.path, pick.body, id)
	}
}
