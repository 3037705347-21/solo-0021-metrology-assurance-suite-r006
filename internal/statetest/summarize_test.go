package statetest_test

import "fmt"

// summarizeDetail renders the clock-independent facts of a case into stable,
// comparable lines for the determinism check: ids, state, version, per
// measurement acceptance and the full event-kind walk.
func summarizeDetail(detail caseDetail) []string {
	header := fmt.Sprintf("case=%s device=%s state=%s version=%d seal=%v measurements=%d events=%d",
		detail.Case.ID, detail.Case.DeviceID, detail.Case.State, detail.Case.Version,
		detail.Seal != nil, len(detail.Measurements), len(detail.Events))
	lines := []string{header}
	for _, m := range detail.Measurements {
		lines = append(lines, fmt.Sprintf("  m id=%s accepted=%v deviation=%v", m.ID, m.Accepted, m.Deviation))
	}
	for i, e := range detail.Events {
		lines = append(lines, fmt.Sprintf("  e %d id=%s kind=%s actor=%s subject=%s", i, e.ID, e.Kind, e.Actor, e.SubjectID))
	}
	if detail.Seal != nil {
		lines = append(lines, fmt.Sprintf("  seal id=%s measurement=%s reviewer=%s",
			detail.Seal.ID, detail.Seal.MeasurementID, detail.Seal.Reviewer))
	}
	return lines
}
