package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/lb"
	"github.com/castisdev/cdn-simul/simul"
	"github.com/castisdev/cdn/profile"
	"github.com/syndtr/goleveldb/leveldb"
)

// NewVODSelector :
func NewVODSelector(algorithm string, hotListUpdatePeriod time.Duration, hotRankLimit int) lb.VODSelector {
	switch algorithm {
	case "weight-storage-bps":
		return &lb.WeightStorageBps{}
	case "dup2":
		return &lb.SameWeightDup2{}
	case "weight-storage":
		return &lb.WeightStorage{}
	case "high-low":
		return lb.NewHighLowGroup(hotListUpdatePeriod, hotRankLimit)
	}
	return &lb.SameHashingWeight{}
}

func main() {
	var cfgFile, dbFile, cpuprofile, memprofile, lp, dbAddr, dbName, lb, hotListUpdatePeriod string
	var readEventCount, hotRankLimit int

	flag.StringVar(&cfgFile, "cfg", "cdn-simul.json", "config file")
	flag.StringVar(&dbFile, "db", "chunk.db", "event db")
	flag.IntVar(&readEventCount, "event-count", 0, "event count to process. if 0, process all event")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile")
	flag.StringVar(&memprofile, "memprofile", "", "write memory profile")
	flag.StringVar(&lp, "log-period", "0s", "status logging period (second). if 0, print log after every event")
	flag.StringVar(&dbAddr, "db-addr", "", "DB address. if empty, not use DB. ex: localhost:8086")
	flag.StringVar(&dbName, "db-name", "mydb", "database name")
	flag.StringVar(&lb, "lb", "hash", "hash | weight-storage | weight-storage-bps | dup2 | high-low")
	flag.StringVar(&hotListUpdatePeriod, "hot-period", "24h", "hot list update period (high-low)")
	flag.IntVar(&hotRankLimit, "hot-rank", 100, "rank limit of hot list, that contents will be served in high group (high-low)")

	flag.Parse()

	logPeriod, err := time.ParseDuration(lp)
	if err != nil {
		log.Fatal(err)
	}

	opt := simul.Options{
		MaxReadEventCount:   readEventCount,
		InfluxDBAddr:        dbAddr,
		InfluxDBName:        dbName,
		SnapshotWritePeriod: logPeriod,
	}

	if cpuprofile != "" {
		if err := profile.StartCPUProfile(cpuprofile); err != nil {
			log.Fatalf("failed to start cpu profile, %v", err)
		}
		defer profile.StopCPUProfile()
	}

	f, err := os.OpenFile(cfgFile, os.O_RDONLY, 0755)
	if err != nil {
		log.Fatalf("failed to open cfg, %v", err)
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("failed to read cfg, %v", err)
	}
	cfg := data.Config{}
	if err := json.Unmarshal(b, &cfg); err != nil {
		log.Fatalf("failed to unmarsharl cfg json, %v", err)
	}

	var writer simul.StatusWriter
	if opt.InfluxDBAddr != "" {
		writer = simul.NewMultiStatusWriter([]simul.StatusWriter{&simul.DBStatusWriter{}, &simul.StdStatusWriter{}})
	} else {
		writer = &simul.StdStatusWriter{}
	}

	db, err := leveldb.OpenFile(dbFile, nil)
	if err != nil {
		log.Fatalf("failed to open db, %v", err)
	}
	defer db.Close()

	du, err := time.ParseDuration(hotListUpdatePeriod)
	if err != nil {
		log.Fatalf("failed to parse duration: %v", err)
	}
	si := simul.NewSimulator(cfg, opt, NewVODSelector(lb, du, hotRankLimit), simul.NewDBEventReader(db), writer)

	now := time.Now()
	si.Run()
	log.Printf("completed. elapsed:%v\n", time.Since(now))

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatalf("failed to start memory profile, %v", err)
		}
		pprof.WriteHeapProfile(f)
		defer f.Close()
	}
}
