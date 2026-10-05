package statetest_test

import "time"

// The structs below mirror the public JSON contracts. They are declared
// locally on purpose: the oracle must be built from the documented wire shape,
// not by importing production domain types, so that a production change that
// silently rewrites a view shows up as a mismatch instead of being hidden by
// a shared type.

type viewDevice struct {
	ID         string     `json:"id"`
	AssetTag   string     `json:"asset_tag"`
	Model      string     `json:"model"`
	Room       string     `json:"room"`
	Steward    string     `json:"steward"`
	Status     string     `json:"status"`
	EnrolledAt time.Time  `json:"enrolled_at"`
	RetiredAt  *time.Time `json:"retired_at,omitempty"`
}

type viewCase struct {
	ID        string    `json:"id"`
	DeviceID  string    `json:"device_id"`
	Nominal   float64   `json:"nominal"`
	Tolerance float64   `json:"tolerance"`
	Steward   string    `json:"steward"`
	State     string    `json:"state"`
	Version   int       `json:"version"`
	OpenedAt  time.Time `json:"opened_at"`
	UpdatedAt time.Time `json:"updated_at"`
	SealedAt  time.Time `json:"sealed_at,omitempty"`
}

type viewMeasurement struct {
	ID         string    `json:"id"`
	CaseID     string    `json:"case_id"`
	Observed   float64   `json:"observed"`
	Nominal    float64   `json:"nominal"`
	Deviation  float64   `json:"deviation"`
	Tolerance  float64   `json:"tolerance"`
	Accepted   bool      `json:"accepted"`
	Technician string    `json:"technician"`
	CreatedAt  time.Time `json:"created_at"`
}

type viewEvent struct {
	ID          string    `json:"id"`
	SubjectType string    `json:"subject_type"`
	SubjectID   string    `json:"subject_id"`
	CaseID      string    `json:"case_id,omitempty"`
	Kind        string    `json:"kind"`
	Actor       string    `json:"actor"`
	Detail      string    `json:"detail"`
	CreatedAt   time.Time `json:"created_at"`
}

type viewSeal struct {
	ID            string    `json:"id"`
	CaseID        string    `json:"case_id"`
	DeviceID      string    `json:"device_id"`
	MeasurementID string    `json:"measurement_id"`
	Reviewer      string    `json:"reviewer"`
	Summary       string    `json:"summary"`
	SealedAt      time.Time `json:"sealed_at"`
}

// caseDetail is the full GET /cases/{id} view.
type caseDetail struct {
	Case         viewCase          `json:"case"`
	Measurements []viewMeasurement `json:"measurements"`
	Seal         *viewSeal         `json:"seal,omitempty"`
	Events       []viewEvent       `json:"events"`
}
