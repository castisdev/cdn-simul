package data

import (
	"io/ioutil"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNewFileInfos(t *testing.T) {
	str := `
7,a.mpg,6456984,518687068,2016-02-23T07:34:01
22,b.mpg,6456946,491967484,2016-02-24T07:34:01`
	exp1 := &FileInfo{7, "a.mpg", 518687068, time.Date(2016, 02, 23, 7, 34, 1, 0, time.UTC)}
	exp2 := &FileInfo{22, "b.mpg", 491967484, time.Date(2016, 02, 24, 7, 34, 1, 0, time.UTC)}

	fi, err := NewFileInfos(strings.NewReader(str))
	if err != nil {
		t.Error(err)
		return
	}

	if reflect.DeepEqual(exp1, fi.Infos[7]) == false {
		t.Errorf("%v != %v", exp1, fi.Infos[7])
	}
	if reflect.DeepEqual(exp2, fi.Infos[22]) == false {
		t.Errorf("%v != %v", exp2, fi.Infos[22])
	}
	if 7 != fi.Keys["a.mpg"] {
		t.Errorf("%v != %v", 7, fi.Keys["a.mpg"])
	}
	if 22 != fi.Keys["b.mpg"] {
		t.Errorf("%v != %v", 22, fi.Keys["b.mpg"])
	}
}

func TestLoadFromLBHistory(t *testing.T) {
	fpath := "test.hitcount.history"

	prepareAndLoad := func(data string) ([]string, error) {
		err := ioutil.WriteFile(fpath, []byte(data), 0777)
		if err != nil {
			return nil, err
		}
		return LoadFromLBHistory(fpath)
	}

	data := `historyheader:1498816756
160412185439000HD.mpg,1460454952,747400,5626862,172.16.45.13,1,0,0,1=0 0
160414101502000HD.mpg,1460596574,747400,5626862,172.16.45.13,1,0,0,1=0 0`
	v, err := prepareAndLoad(data)
	if err != nil {
		t.Error(err)
		return
	}

	if v[0] != "160412185439000HD.mpg" {
		t.Errorf("%v != %v", "160412185439000HD.mpg", v[0])
	}
	if v[1] != "160414101502000HD.mpg" {
		t.Errorf("%v != %v", "160414101502000HD.mpg", v[1])
	}

}
