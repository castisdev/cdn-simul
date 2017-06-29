package simul

import (
	"testing"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/glblog"
	"github.com/castisdev/cdn-simul/lb"
)

func TestSimulator_Run_Simple(t *testing.T) {
	cfg := data.Config{
		VODs: []data.VODConfig{data.VODConfig{VodID: "vod1", StorageSize: 1000000000, LimitSession: 10000, LimitBps: 1000000000000}},
	}
	ss := []*glblog.SessionInfo{
		&glblog.SessionInfo{
			SID:       "sess-A",
			Started:   StrToTime("2017-04-29 08:16:37.499"),
			Ended:     StrToTime("2017-04-29 08:16:39.015"),
			Filename:  "a.mpg",
			Bandwidth: 10552998,
			Offset:    0,
		},
		&glblog.SessionInfo{
			SID:       "sess-B",
			Started:   StrToTime("2017-04-29 08:16:39.012"),
			Ended:     StrToTime("2017-04-29 08:16:42.096"),
			Filename:  "B.mpg",
			Bandwidth: 6459282,
			Offset:    376,
		},
	}

	lb, err := lb.New(cfg, &lb.SameHashingWeight{})
	if err != nil {
		t.Errorf("failed to create loadbalancer instance")
		return
	}
	si := NewSimulator(cfg, Options{}, lb, NewTestEventReader(ss), &StdStatusWriter{}, nil)
	if si == nil {
		t.Errorf("failed to create simulator instance")
		return
	}

	si.Run()
}

func TestSimulator_Run_SameWeightDup2(t *testing.T) {
	limitBps := int64(1000 * 1000 * 100)
	cfg := data.Config{
		VODs: []data.VODConfig{
			data.VODConfig{VodID: "vod1", StorageSize: 1000000000, LimitSession: 10, LimitBps: limitBps},
			data.VODConfig{VodID: "vod2", StorageSize: 1000000000, LimitSession: 10000, LimitBps: limitBps * 10},
		},
	}
	ss := []*glblog.SessionInfo{
		&glblog.SessionInfo{
			SID:       "sess-A",
			Started:   StrToTime("2017-04-29 08:16:37.499"),
			Ended:     StrToTime("2017-04-29 08:16:39.015"),
			Filename:  "a.mpg",
			Bandwidth: 10000000,
			Offset:    0,
		},
		&glblog.SessionInfo{
			SID:       "sess-B",
			Started:   StrToTime("2017-04-29 08:16:39.012"),
			Ended:     StrToTime("2017-04-29 08:16:42.096"),
			Filename:  "B.mpg",
			Bandwidth: 1000000,
			Offset:    0,
		},
	}

	lb, err := lb.New(cfg, &lb.SameWeightDup2{})
	if err != nil {
		t.Errorf("failed to create loadbalancer instance")
		return
	}
	si := NewSimulator(cfg, Options{}, lb, NewTestEventReader(ss), nil, nil)
	if si == nil {
		t.Errorf("failed to create simulator instance")
		return
	}

	si.Run()
}

func TestSimulator_Run_Bypass(t *testing.T) {
	cfg := data.Config{
		VODs: []data.VODConfig{data.VODConfig{VodID: "vod1", StorageSize: 1000000000, LimitSession: 10000, LimitBps: 1000000000000}},
	}
	ss := []*glblog.SessionInfo{
		&glblog.SessionInfo{
			SID:       "sess-1",
			Started:   StrToTime("2017-01-01 00:00:00.000"),
			Ended:     StrToTime("2017-01-01 00:00:05.000"),
			Filename:  "a.mpg",
			Bandwidth: 1000000,
			Offset:    0,
		},
		&glblog.SessionInfo{
			SID:       "sess-2",
			Started:   StrToTime("2017-01-01 00:00:01.000"),
			Ended:     StrToTime("2017-01-01 00:00:02.000"),
			Filename:  "a.mpg",
			Bandwidth: 1000000,
			Offset:    0,
		},
		&glblog.SessionInfo{
			SID:       "sess-3",
			Started:   StrToTime("2017-01-01 00:00:01.000"),
			Ended:     StrToTime("2017-01-01 00:00:02.000"),
			Filename:  "b.mpg",
			Bandwidth: 1000000,
			Offset:    0,
		},
		&glblog.SessionInfo{
			SID:       "sess-4",
			Started:   StrToTime("2017-01-01 00:00:02.000"),
			Ended:     StrToTime("2017-01-01 00:00:03.000"),
			Filename:  "c.mpg",
			Bandwidth: 1000000,
			Offset:    0,
		},
		&glblog.SessionInfo{
			SID:       "sess-5",
			Started:   StrToTime("2017-01-01 00:00:03.000"),
			Ended:     StrToTime("2017-01-01 00:00:04.000"),
			Filename:  "c.mpg",
			Bandwidth: 1000000,
			Offset:    0,
		},
	}

	lb, err := lb.New(cfg, &lb.SameHashingWeight{})
	if err != nil {
		t.Errorf("failed to create loadbalancer instance")
		return
	}
	bypass := []string{"a.mpg", "b.mpg"}

	si := NewSimulator(cfg, Options{}, lb, NewTestEventReader(ss), &StdStatusWriter{}, bypass)
	if si == nil {
		t.Errorf("failed to create simulator instance")
		return
	}

	si.Run()
}

func TestSimulator_Run_FirstBypass(t *testing.T) {
	limitBps := int64(1000 * 1000 * 100)
	cfg := data.Config{
		VODs: []data.VODConfig{
			data.VODConfig{VodID: "vod1", StorageSize: 1000000000, LimitSession: 10, LimitBps: limitBps},
		},
	}
	ss := []*glblog.SessionInfo{
		&glblog.SessionInfo{
			SID:       "sess-A",
			Started:   StrToTime("2017-01-01 00:00:00.000"),
			Ended:     StrToTime("2017-01-01 00:00:01.000"),
			Filename:  "a.mpg",
			Bandwidth: 10000000,
			Offset:    0,
		},
		&glblog.SessionInfo{
			SID:       "sess-B",
			Started:   StrToTime("2017-01-01 00:00:02.000"),
			Ended:     StrToTime("2017-01-01 00:00:03.000"),
			Filename:  "a.mpg",
			Bandwidth: 10000000,
			Offset:    0,
		},
		&glblog.SessionInfo{
			SID:       "sess-C",
			Started:   StrToTime("2017-01-01 01:01:00.000"),
			Ended:     StrToTime("2017-01-01 01:01:01.000"),
			Filename:  "a.mpg",
			Bandwidth: 10000000,
			Offset:    0,
		},
		&glblog.SessionInfo{
			SID:       "sess-D",
			Started:   StrToTime("2017-01-01 01:01:02.000"),
			Ended:     StrToTime("2017-01-01 01:01:03.000"),
			Filename:  "a.mpg",
			Bandwidth: 10000000,
			Offset:    0,
		},
	}
	lb, err := lb.New(cfg, &lb.SameHashingWeight{})
	if err != nil {
		t.Errorf("failed to create loadbalancer instance")
		return
	}
	si := NewSimulator(cfg, Options{FirstBypass: true, FBPeriod: time.Hour}, lb, NewTestEventReader(ss), &StdStatusWriter{}, nil)
	if si == nil {
		t.Errorf("failed to create simulator instance")
		return
	}

	si.Run()
}

func TestSimulator_Run_Legacy(t *testing.T) {
	limitBps := int64(1000 * 1000 * 100)
	cfg := data.Config{
		VODs: []data.VODConfig{
			data.VODConfig{VodID: "vod1", StorageSize: 1000000000, LimitSession: 10, LimitBps: limitBps},
		},
	}
	ss := []*glblog.SessionInfo{
		&glblog.SessionInfo{
			SID:       "sess-A",
			Started:   StrToTime("2017-01-01 00:00:00.000"),
			Ended:     StrToTime("2017-01-01 00:00:01.000"),
			Filename:  "a.mpg",
			Bandwidth: 10000000,
			Offset:    0,
			IsCenter:  true,
		},
		&glblog.SessionInfo{
			SID:       "sess-B",
			Started:   StrToTime("2017-01-01 00:00:02.000"),
			Ended:     StrToTime("2017-01-01 00:00:03.000"),
			Filename:  "a.mpg",
			Bandwidth: 10000000,
			Offset:    0,
			IsCenter:  false,
		},
	}
	lb, err := lb.NewLegacyLB(cfg, &lb.SameHashingWeight{})
	if err != nil {
		t.Errorf("failed to create loadbalancer instance")
		return
	}

	si := NewSimulator(cfg, Options{FirstBypass: true, FBPeriod: time.Hour}, lb, NewTestEventReader(ss), &StdStatusWriter{}, nil)
	if si == nil {
		t.Errorf("failed to create simulator instance")
		return
	}

	si.Run()
}
