package lb

import (
	"log"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/castisdev/cdn-simul/data"
)

var layout = "2006-01-02 15:04:05"

// StrToTime :
func StrToTime(str string) time.Time {
	loc, _ := time.LoadLocation("Local")
	t, err := time.ParseInLocation(layout, str, loc)
	if err != nil {
		log.Fatal(err)
	}
	return t
}

func TestHitRanker(t *testing.T) {

	str := `
1,a.mpg,6000000,500000000,2017-01-01T00:00:00
2,b.mpg,6000000,500000000,2016-01-01T00:00:00
3,c.mpg,6000000,500000000,2015-01-01T00:00:00
4,d.mpg,6000000,500000000,2014-01-01T00:00:00
5,e.mpg,6000000,500000000,2013-01-01T00:00:00`

	id := func(file string) int {
		switch file {
		case "a.mpg":
			return 1
		case "b.mpg":
			return 2
		case "c.mpg":
			return 3
		case "d.mpg":
			return 4
		case "e.mpg":
			return 5
		default:
			log.Fatal("invalid test data")
		}
		return 0
	}

	fi, err := data.NewFileInfos(strings.NewReader(str))
	if err != nil {
		t.Error(err)
		return
	}

	statDuration := 24 * time.Hour
	shiftPeriod := 1 * time.Hour
	dc := NewHitRanker(statDuration, shiftPeriod, fi)

	eventFn := func(file, strTime, sid string) {
		evt := &data.SessionEvent{
			FileName:    file,
			SessionID:   sid,
			Time:        StrToTime(strTime),
			IntFileName: id(file),
			FileSize:    500000000,
			Bps:         6000000,
			Duration:    time.Minute,
		}

		dc.Update(evt)
	}

	deletableFn := func(name string, curContents map[int]struct{}, delSize int64, expected []int) {
		ret := dc.Deletable(curContents, delSize)
		if reflect.DeepEqual(expected, ret) == false {
			t.Errorf("[%v] %v != %v", name, expected, ret)
		}
	}
	addableFn := func(name string, curContents map[int]struct{}, storageSize int64, wantErr bool, expected int) {
		ret, _, err := dc.Addable(curContents, storageSize, nil)
		if (err != nil) != wantErr {
			t.Errorf("[%v] %v != %v", name, wantErr, err != nil)
			return
		}
		if ret != expected {
			t.Errorf("[%v] %v != %v", name, expected, ret)
		}
	}

	var empty struct{}
	curContents := make(map[int]struct{})
	curContents[id("a.mpg")] = empty
	curContents[id("b.mpg")] = empty
	curContents[id("c.mpg")] = empty

	eventFn("a.mpg", "2017-01-01 00:10:00", "s1")
	deletableFn("deletable size 1", curContents, 500000000, []int{id("c.mpg")})
	deletableFn("deletable size 2", curContents, 100000000, []int{id("c.mpg")})
	deletableFn("deletable size 3", curContents, 700000000, []int{id("c.mpg"), id("b.mpg")})
	deletableFn("deletable size 4", curContents, 1500000000, []int{id("c.mpg"), id("b.mpg"), id("a.mpg")})

	eventFn("c.mpg", "2017-01-01 01:50:00", "s2")
	deletableFn("deletable after c.mpg hit", curContents, 2000000000, []int{id("b.mpg"), id("c.mpg"), id("a.mpg")})

	eventFn("d.mpg", "2017-01-02 00:50:00", "s3")
	deletableFn("deletable after 24h, deleted a.mpg hit", curContents, 2000000000, []int{id("b.mpg"), id("a.mpg"), id("c.mpg")})

	eventFn("d.mpg", "2017-01-02 02:10:00", "s4")
	deletableFn("deletable after deleted c.mpg hit", curContents, 2000000000, []int{id("c.mpg"), id("b.mpg"), id("a.mpg")})

	// after 1 month.. deleted previous hit info

	eventFn("c.mpg", "2017-02-01 00:00:00", "s3")
	addableFn("addable c.mpg hit, but already exists", curContents, 1500000000, true, 0)

	eventFn("d.mpg", "2017-02-01 00:10:00", "s4")
	addableFn("addable after d.mpg hit", curContents, 1500000000, false, id("d.mpg"))

	eventFn("e.mpg", "2017-02-01 00:20:00", "s5")
	addableFn("addable after e.mpg hit (expected file registred later)", curContents, 1500000000, false, id("d.mpg"))

	eventFn("e.mpg", "2017-02-01 00:30:00", "s6")
	addableFn("addable after e.mpg hit 2", curContents, 1500000000, false, id("e.mpg"))
}