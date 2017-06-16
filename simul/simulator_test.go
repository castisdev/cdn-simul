package simul

import (
	"testing"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/glblog"
	"github.com/castisdev/cdn-simul/lb"
)

func TestSimulator_Run(t *testing.T) {
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
	si := NewSimulator(cfg, Options{}, &lb.SameHashingWeight{}, NewTestEventReader(ss), nil)
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
	si := NewSimulator(cfg, Options{}, &lb.SameWeightDup2{}, NewTestEventReader(ss), nil)
	if si == nil {
		t.Errorf("failed to create simulator instance")
		return
	}

	si.Run()
}