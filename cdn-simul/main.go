package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	"strings"
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
	var cfgFile, dbFile, cpuprofile, memprofile, lp, dbAddr, dbName, lb, hotListUpdatePeriod, bypass, fbPeriod, simulID string
	var readEventCount, hotRankLimit int
	var firstBypass bool

	flag.StringVar(&cfgFile, "cfg", "cdn-simul.json", "config file")
	flag.StringVar(&dbFile, "db", "chunk.db", "event db")
	flag.IntVar(&readEventCount, "event-count", 0, "event count to process. if 0, process all event")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile")
	flag.StringVar(&memprofile, "memprofile", "", "write memory profile")
	flag.StringVar(&lp, "log-period", "0s", "status logging period (second). if 0, print log after every event")
	flag.StringVar(&dbAddr, "db-addr", "", "DB address. if empty, not use DB. ex: localhost:8086")
	flag.StringVar(&dbName, "db-name", "cdn-simul", "database name")
	flag.StringVar(&lb, "lb", "hash", "hash | weight-storage | weight-storage-bps | dup2 | high-low")
	flag.StringVar(&hotListUpdatePeriod, "hot-period", "24h", "hot list update period (high-low)")
	flag.IntVar(&hotRankLimit, "hot-rank", 100, "rank limit of hot list, that contents will be served in high group (high-low)")
	flag.StringVar(&bypass, "bypass", "", "text file that has contents list to bypass")
	flag.BoolVar(&firstBypass, "first-bypass", false, "if true, chunks of first hit session for 24h will be bypassed")
	flag.StringVar(&fbPeriod, "fb-period", "24h", "first bypass list update period (only used with first-bypass option)")
	flag.StringVar(&simulID, "id", "cdn-simul", "simulation id, that used with tag values in influx DB")

	flag.Parse()

	logPeriod, err := time.ParseDuration(lp)
	if err != nil {
		log.Fatal(err)
	}
	fbp, err := time.ParseDuration(fbPeriod)
	if err != nil {
		log.Fatal(err)
	}

	opt := simul.Options{
		MaxReadEventCount: readEventCount,
		InfluxDBAddr:      dbAddr,
		InfluxDBName:      dbName,
		StatusWritePeriod: logPeriod,
		BypassFile:        bypass,
		FirstBypass:       firstBypass,
		FBPeriod:          fbp,
		SimulID:           simulID,
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

	var bypassList []string
	if opt.BypassFile != "" {
		b, err := ioutil.ReadFile(opt.BypassFile)
		if err != nil {
			log.Fatalf("failed to read bypass file, %v", f)
		}
		bypassList = strings.Split(string(b), "\n")
	}

	du, err := time.ParseDuration(hotListUpdatePeriod)
	if err != nil {
		log.Fatalf("failed to parse duration: %v", err)
	}
	si := simul.NewSimulator(cfg, opt, NewVODSelector(lb, du, hotRankLimit), simul.NewDBEventReader(db), writer, bypassList)

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
