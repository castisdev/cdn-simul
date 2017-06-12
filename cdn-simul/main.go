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
	"github.com/castisdev/cdn-simul/simul"
	"github.com/castisdev/cdn/profile"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	var cfgFile, dbFile, cpuprofile, memprofile, lp string
	var dbAddr, dbUser, dbPass, dbName string
	var readEventCount int
	var writeDB bool

	flag.StringVar(&cfgFile, "cfg", "cdn-simul.json", "config file")
	flag.StringVar(&dbFile, "db", "chunk.db", "event db")
	flag.IntVar(&readEventCount, "event-count", 0, "event count to process. if 0, process all event")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile")
	flag.StringVar(&memprofile, "memprofile", "", "write memory profile")
	flag.StringVar(&lp, "log-period", "0s", "status logging period (second). if 0, print log after every event")
	flag.StringVar(&dbAddr, "db-addr", "", "DB address. if empty, not use DB. ex: localhost:8086")
	flag.StringVar(&dbUser, "db-user", "", "DB username")
	flag.StringVar(&dbPass, "db-pass", "", "DB password")
	flag.StringVar(&dbName, "db-name", "mydb", "database name")

	flag.Parse()

	logPeriod, err := time.ParseDuration(lp)
	if err != nil {
		log.Fatal(err)
	}

	opt := simul.Options{
		MaxReadEventCount:   readEventCount,
		UseWriteInfluxDB:    writeDB,
		InfluxDBAddr:        dbAddr,
		InfluxDBName:        dbName,
		InfluxDBUser:        dbUser,
		InfluxDBPass:        dbPass,
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
	if opt.UseWriteInfluxDB {
		writer = simul.NewMultiStatusWriter([]simul.StatusWriter{&simul.DBStatusWriter{}, &simul.StdStatusWriter{}})
	} else {
		writer = &simul.StdStatusWriter{}
	}

	db, err := leveldb.OpenFile(dbFile, nil)
	if err != nil {
		log.Fatalf("failed to open db, %v", err)
	}
	si := simul.NewSimulator(cfg, opt, simul.NewDBEventReader(db), writer)

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
