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
	"github.com/castisdev/cdn-simul/simul"
	"github.com/castisdev/cdn/profile"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	var cfgFile, dbFile, cpuprofile, memprofile, lp, dbAddr, dbName, lbType, hotListUpdatePeriod, bypass, fbPeriod, simulID, start string
	var statDu, statDuDel, shiftP, pushP, fiFilepath, lbHistory, adsFile, purgeFile string
	var readEventCount, hotRankLimit, pushDelayN, dawnPushN int
	var firstBypass, useSessionDu, useDeleteLru, useFileSize, useTimeWeight bool

	flag.StringVar(&cfgFile, "cfg", "cdn-simul.json", "config file")
	flag.StringVar(&dbFile, "db", "chunk.db", "event db")
	flag.IntVar(&readEventCount, "event-count", 0, "event count to process. if 0, process all event")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile")
	flag.StringVar(&memprofile, "memprofile", "", "write memory profile")
	flag.StringVar(&lp, "log-period", "0s", "status logging period (second). if 0, print log after every event")
	flag.StringVar(&dbAddr, "db-addr", "", "DB address. if empty, not use DB. ex: localhost:8086")
	flag.StringVar(&dbName, "db-name", "cdn-simul", "database name")
	flag.StringVar(&lbType, "lb", "hash", "hash | weight-storage | weight-storage-bps | dup2 | high-low | legacy | filebase")
	flag.StringVar(&hotListUpdatePeriod, "hot-period", "24h", "hot list update period (high-low)")
	flag.IntVar(&hotRankLimit, "hot-rank", 100, "rank limit of hot list, that contents will be served in high group (high-low)")
	flag.StringVar(&statDu, "stat-range", "24h", "data collect window size (filebase)")
	flag.StringVar(&statDuDel, "stat-range-del", "24h", "data collect window size for delete (filebase)")
	flag.StringVar(&shiftP, "shift-period", "1h", "data collect window shift period (filebase)")
	flag.StringVar(&pushP, "push-period", "5m", "file push period (filebase)")
	flag.IntVar(&pushDelayN, "push-delay", 2, "file push delay number, (push time = push-period * push-delay) (filebase)")
	flag.IntVar(&dawnPushN, "dawn-push", 1, "push contents count in the dawn of day (filebase)")
	flag.StringVar(&fiFilepath, "file-info", "fileinfo.csv", "csv file path contains id,name,size,bps,register-time (filebase)")
	flag.StringVar(&lbHistory, "lb-history", "", "LB hitcount history file for initial contents (filebase)")
	flag.StringVar(&adsFile, "ads-csv", "", "ADSAdapter csv file (filebase)")
	flag.StringVar(&purgeFile, "purge-csv", "", "purge csv file (filebase)")
	flag.BoolVar(&useSessionDu, "session-duration", false, "add session duration into hit weight (filebase)")
	flag.BoolVar(&useDeleteLru, "delete-lru", false, "delete file using with LRU (filebase)")
	flag.BoolVar(&useFileSize, "file-size", false, "add file size and session duration into hit weight (filebase)")
	flag.BoolVar(&useTimeWeight, "time-weight", false, "add time weight into hit weight (filebase)")
	flag.StringVar(&bypass, "bypass", "", "text file that has contents list to bypass")
	flag.BoolVar(&firstBypass, "first-bypass", false, "if true, chunks of first hit session for 24h will be bypassed")
	flag.StringVar(&fbPeriod, "fb-period", "24h", "first bypass list update period (only used with first-bypass option)")
	flag.StringVar(&simulID, "id", "cdn-simul", "simulation id, that used with tag values in influx DB")
	flag.StringVar(&start, "start", "", "simulation start point, before that point events will be ignored, (ex)2017-01-01 00:00:00.000")

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
	if start != "" {
		t := simul.StrToTime(start)
		opt.StartTime = t
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

	hlup, err := time.ParseDuration(hotListUpdatePeriod)
	if err != nil {
		log.Fatal(err)
	}
	sd, err := time.ParseDuration(statDu)
	if err != nil {
		log.Fatal(err)
	}
	sdd, err := time.ParseDuration(statDuDel)
	if err != nil {
		log.Fatal(err)
	}
	sp, err := time.ParseDuration(shiftP)
	if err != nil {
		log.Fatal(err)
	}
	pp, err := time.ParseDuration(pushP)
	if err != nil {
		log.Fatal(err)
	}
	var fi *data.FileInfos
	if lbType == "filebase" {
		fiFile, err := os.Open(fiFilepath)
		if err != nil {
			log.Fatal(err)
		}
		defer fiFile.Close()
		fi, err = data.NewFileInfos(fiFile)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fi, err = data.NewEmptyFileInfos()
		if err != nil {
			log.Fatal(err)
		}
	}

	lbOpt := simul.LBOption{
		Cfg:                 cfg,
		LBType:              lbType,
		HotListUpdatePeriod: hlup,
		HotRankLimit:        hotRankLimit,
		StatDuration:        sd,
		StatDurationForDel:  sdd,
		ShiftPeriod:         sp,
		PushPeriod:          pp,
		PushDelayN:          pushDelayN,
		DawnPushN:           dawnPushN,
		Fileinfos:           fi,
		UseSessionDuration:  useSessionDu,
		UseDeleteLru:        useDeleteLru,
		UseFileSize:         useFileSize,
		UseTimeWeight:       useTimeWeight,
	}
	if lbHistory != "" {
		initList, err := data.LoadFromLBHistory(lbHistory)
		if err != nil {
			log.Fatal(err)
		}
		lbOpt.InitContents = initList
	}
	if adsFile != "" {
		adsEv, err := data.LoadFromADSAdapterCsv(adsFile)
		if err != nil {
			log.Fatal(err)
		}
		lbOpt.DeliverEvent = adsEv
	}
	if purgeFile != "" {
		purgeEv, err := data.LoadFromPurgeCsv(purgeFile)
		if err != nil {
			log.Fatal(err)
		}
		lbOpt.PurgeEvent = purgeEv
	}
	alb, err := simul.NewLoadBalancer(lbOpt)
	if err != nil {
		log.Fatalf("failed to create loadbalancer instance: %v", err)
	}
	si := simul.NewSimulator(cfg, opt, alb, simul.NewDBEventReader(db), writer, fi, bypassList)

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
