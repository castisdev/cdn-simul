package clientip

import (
	"strings"
	"testing"
)

func TestChecker_Check(t *testing.T) {
	csv := `
172.16.45.11,172.16.46.22,23
172.16.40.10,172.16.42.10,23`

	checker, err := NewChecker(strings.NewReader(csv))
	if err != nil {
		t.Error(err)
		return
	}

	testFn := func(ip string, expected bool) {
		if v := checker.Check(ip); v != expected {
			t.Errorf("[%s] %v != %v", ip, expected, v)
		}
	}

	testFn("172.16.45.10", false)
	testFn("172.16.45.11", true)
	testFn("172.16.46.01", true)
	testFn("172.16.46.22", true)
	testFn("172.16.46.23", false)
	testFn("172.16.1.23", false)
	testFn("172.16.40.09", false)
	testFn("172.16.40.10", true)
}
