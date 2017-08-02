package clientip

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net"
)

// IPRange :
type IPRange struct {
	startIP string
	endIP   string
}

// Checker :
type Checker struct {
	ipRanges []IPRange
}

// NewChecker : csv format [start-ip,end-ip,masking-bit]
//   csv 생성 : (예: 강북 ip 리스트 생성) cut -d'|' -f1,5,6,8 FILE_TB_ASSIGN.DAT| egrep "R00451|R00449|R00450|R00430|R00452" |cut -d'|' -f1,2,4|sed 's/|/,/g' > kangbuk-ip.csv
//   http://alice/castis/ipms-importer/ 의 data 참조
func NewChecker(reader io.Reader) (*Checker, error) {
	r := csv.NewReader(reader)

	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read ip list, %v", err)
	}
	c := &Checker{ipRanges: make([]IPRange, 0)}
	for _, v := range records {
		c.ipRanges = append(c.ipRanges, IPRange{startIP: v[0], endIP: v[1]})
	}
	return c, nil
}

// Check :
func (c *Checker) Check(ip string) bool {
	v := net.ParseIP(ip)
	for _, r := range c.ipRanges {
		start := net.ParseIP(r.startIP)
		end := net.ParseIP(r.endIP)
		if start == nil || end == nil {
			log.Fatalf("failed to parse ip, start:%v, end:%v\n", r.startIP, r.endIP)
		}
		if bytes.Compare(v, start) >= 0 && bytes.Compare(v, end) <= 0 {
			return true
		}
	}
	return false
}
