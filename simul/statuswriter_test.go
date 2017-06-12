package simul

import (
	"testing"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/status"
)

type testWriter struct {
	called bool
}

func (w *testWriter) WriteStatus(ti time.Time, st status.Status, cfg data.Config, opt Options) {
	w.called = true
}
func TestMultiStatusWriter_WriteStatus(t *testing.T) {
	a := &testWriter{}
	b := &testWriter{}
	w := NewMultiStatusWriter([]StatusWriter{a, b})
	w.WriteStatus(StrToTime("2017-04-29 08:16:37.499"), status.Status{}, data.Config{}, Options{})
	if !a.called {
		t.Errorf("not called write")
	}
	if !b.called {
		t.Errorf("not called write")
	}
}
