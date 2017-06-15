package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/castisdev/cdn-simul/glblog"
	"github.com/castisdev/cdn-simul/simul"
	client "github.com/influxdata/influxdb/client/v2"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	dbdir := flag.String("in-db-dir", "", "level db directory for input ")
	outDBAddr := flag.String("out-db-addr", "", "influx db address for out")
	outDBName := flag.String("out-db-name", "", "influx db database name")
	filter := flag.String("filter", "", "text file, each line is filename")

	flag.Parse()

	var filters []string
	if *filter != "" {
		b, err := ioutil.ReadFile(*filter)
		if err != nil {
			log.Fatal(err)
		}
		filters = strings.Split(string(b), "\n")
	}
	isOK := func(a string) bool {
		if len(filters) == 0 {
			return true
		}
		for _, v := range filters {
			if v == a {
				return true
			}
		}
		return false
	}

	db, err := leveldb.OpenFile(*dbdir, nil)
	if err != nil {
		log.Fatal(err)
	}

	in := simul.NewDBEventReader(db)

	cl, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://" + *outDBAddr,
	})
	if err != nil {
		log.Fatal(err)
	}

	count := 0
	var bp client.BatchPoints
	bp = nil
	for {
		if count%1000 == 0 {
			if bp != nil {
				if err := cl.Write(bp); err != nil {
					log.Fatal(err)
				}
				fmt.Println("write to DB")
			}
			bp, err = client.NewBatchPoints(client.BatchPointsConfig{
				Database:  *outDBName,
				Precision: "s",
			})
			if err != nil {
				log.Fatal(err)
			}
		}
		ev := in.ReadEvent()
		if ev == nil {
			if len(bp.Points()) > 0 {
				if err := cl.Write(bp); err != nil {
					log.Fatal(err)
				}
				fmt.Println("last write to DB")
			}
			break
		}
		if isOK(ev.Filename) {
			AddToDB(bp, ev)
			fmt.Println(ev)
			count++
		}
	}
}

// AddToDB :
func AddToDB(bp client.BatchPoints, ev *glblog.SessionInfo) {
	tags := map[string]string{"file": ev.Filename}
	fields := map[string]interface{}{
		"sid":          ev.SID,
		"bps":          ev.Bandwidth,
		"ended":        ev.Ended,
		"offset":       ev.Offset,
		"duraiton-sec": ev.Ended.Sub(ev.Started).Seconds(),
	}

	pt, err := client.NewPoint("session", tags, fields, ev.Started)
	if err != nil {
		log.Fatal(err)
	}
	bp.AddPoint(pt)
}
