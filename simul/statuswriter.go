package simul

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/lb/vod"
	"github.com/castisdev/cdn-simul/status"
	humanize "github.com/dustin/go-humanize"
)

// StatusWriter :
type StatusWriter interface {
	WriteStatus(ti time.Time, st status.Status, cfg data.Config, opt Options, gst *stat)
}

// DBStatusWriter : write status to Influx DB
type DBStatusWriter struct{}

// WriteStatus :
func (w DBStatusWriter) WriteStatus(ti time.Time, st status.Status, cfg data.Config, opt Options, gst *stat) {
	str := ""
	t := ti.UnixNano()
	for _, v := range st.Caches {
		vcfg := FindConfig(&cfg, v.VODKey)
		vod := st.Vods[vod.Key(v.VODKey)]
		str += fmt.Sprintf("cache,vod=%s hit=%d,miss=%d,originbps=%d,disk=%d,disklimit=%d %d\n",
			v.VODKey, v.CacheHitCount, v.CacheMissCount, v.OriginBps, v.CurSize, vcfg.StorageSize, t)
		str += fmt.Sprintf("vod,vod=%s bps=%d,bpslimit=%d,session=%d,sessionlimit=%d %d\n",
			v.VODKey, vod.CurBps, vcfg.LimitBps, vod.CurSessionCount, vcfg.LimitSession, t)
	}

	reqBody := bytes.NewBufferString(str)
	cl := &http.Client{
		Timeout: 3 * time.Second,
		// http.DefaultTransport + (DisableKeepAlives: true)
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableKeepAlives:     true,
		},
	}

	req, err := http.NewRequest("POST", "http://"+opt.InfluxDBAddr+"/write?db="+opt.InfluxDBName, reqBody)
	if err != nil {
		log.Fatalf("failed to creat request, %v\n", err)
		return
	}
	resp, err := cl.Do(req)
	if err != nil {
		log.Fatalf("failed to post request, %v\n", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := ioutil.ReadAll(resp.Body)
		log.Fatalf("failed to post request, status:%s, body:%s\n", resp.Status, string(b))
		return
	}
}

// StdStatusWriter :
type StdStatusWriter struct{}

// WriteStatus :
func (w *StdStatusWriter) WriteStatus(ti time.Time, st status.Status, cfg data.Config, opt Options, gst *stat) {
	str := ""
	totalHit := int64(0)
	totalMiss := int64(0)

	hitRateFn := func(hit, miss int64) int {
		if hit == 0 {
			return 0
		}
		return int(float64(hit) * 100 / float64(hit+miss))
	}
	// cfg의 VOD 순으로 logging
	for _, cc := range cfg.VODs {
		v := st.Vods[vod.Key(cc.VodID)]
		cache := FindCacheStatus(&st, v.VODKey)
		vc := FindConfig(&cfg, v.VODKey)

		hit := cache.CacheHitCount - gst.vods[v.VODKey].hitCountWhenReset
		miss := cache.CacheMissCount - gst.vods[v.VODKey].missCountWhenReset
		totalHit += hit
		totalMiss += miss
		str += fmt.Sprintf("%s [%15s session(%4v/%4v/%3v%%/max:%3v%%) bps(%7v/%7v/%3v%%/max:%3v%%) disk(%8v/%8v/%3v%%) hit(%5v/%5v: %3v %%) origin(%6v)]\n",
			st.Time.Format(layout),
			v.VODKey, v.CurSessionCount, vc.LimitSession, int(float64(v.CurSessionCount)*100/float64(vc.LimitSession)), gst.vods[v.VODKey].maxSessionPercent,
			humanize.Bytes(uint64(v.CurBps)), humanize.Bytes(uint64(vc.LimitBps)), int(float64(v.CurBps)*100/float64(vc.LimitBps)), gst.vods[v.VODKey].maxBpsPercent,
			humanize.IBytes(uint64(cache.CurSize)), humanize.IBytes(uint64(vc.StorageSize)), int(float64(cache.CurSize)*100/float64(vc.StorageSize)),
			hit, hit+miss, hitRateFn(hit, miss),
			humanize.Bytes(uint64(cache.OriginBps)))
	}

	str = fmt.Sprintf("\n%s all-full:%v originBps(cur:%4v/max:%4v) hit(%4v/%4v: %3v %%)\n",
		st.Time.Format(layout),
		st.AllCacheFull, humanize.Bytes(uint64(st.Origin.Bps)), humanize.Bytes(uint64(gst.maxOriginBps)),
		totalHit, totalHit+totalMiss, hitRateFn(totalHit, totalMiss)) + str
	fmt.Println(str)
}

// MultiStatusWriter :
type MultiStatusWriter struct {
	writers []StatusWriter
}

// NewMultiStatusWriter :
func NewMultiStatusWriter(w []StatusWriter) *MultiStatusWriter {
	return &MultiStatusWriter{writers: w}
}

// WriteStatus :
func (w *MultiStatusWriter) WriteStatus(ti time.Time, st status.Status, cfg data.Config, opt Options, gst *stat) {
	for _, v := range w.writers {
		v.WriteStatus(ti, st, cfg, opt, gst)
	}
}
