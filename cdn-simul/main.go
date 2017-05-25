package main

import (
	"bytes"
	"encoding/csv"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/lb"
	"github.com/castisdev/cdn/profile"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

const chunkSize int64 = 2 * 1024 * 1024

type eventType int

func (et eventType) String() string {
	switch et {
	case sessionCreated:
		return "created"
	case sessionClosed:
		return "closed"
	default:
		return "unknown"
	}
}

const (
	sessionCreated eventType = iota
	sessionClosed
)

func parseEventType(str string) (eventType, error) {
	switch str {
	case "created":
		return sessionCreated, nil
	case "closed":
		return sessionClosed, nil
	}
	return sessionClosed, fmt.Errorf("unknown event: %v", str)
}

type sessionEvent struct {
	sid       string
	eventTime time.Time
	eventType eventType
	filename  string
	bandwidth int
}

func parseSessionEvent(rec []string) (sessionEvent, error) {
	if len(rec) != 5 {
		return sessionEvent{}, fmt.Errorf("invalid sessionEvent string, %v", rec)
	}
	et, err := parseEventType(strings.Trim(rec[2], " "))
	if err != nil {
		return sessionEvent{}, err
	}
	bps, err := strconv.ParseInt(strings.Trim(rec[4], " "), 10, 64)
	return sessionEvent{
		eventTime: strToTime(strings.Trim(rec[0], " ")),
		sid:       strings.Trim(rec[1], " "),
		eventType: et,
		filename:  strings.Trim(rec[3], " "),
		bandwidth: int(bps),
	}, nil
}

type chunkEvent struct {
	SID       string
	EventTime time.Time
	Filename  string
	Index     int
}

func parseChunkEvent(rec []string) (chunkEvent, error) {
	if len(rec) != 4 {
		return chunkEvent{}, fmt.Errorf("invalid chunkEvent string, %v", rec)
	}
	idx, err := strconv.ParseInt(strings.Trim(rec[2], " "), 10, 64)
	if err != nil {
		return chunkEvent{}, err
	}
	return chunkEvent{
		EventTime: strToTime(strings.Trim(rec[0], " ")),
		SID:       strings.Trim(rec[1], " "),
		Index:     int(idx),
		Filename:  strings.Trim(rec[3], " "),
	}, nil
}

func strToTime(str string) time.Time {
	layout := "2006-01-02 15:04:05.000"
	loc, _ := time.LoadLocation("Local")
	t, _ := time.ParseInLocation(layout, str, loc)
	return t
}

func timeToStr(t time.Time) string {
	layout := "2006-01-02 15:04:05.000"
	return t.Format(layout)
}

func readOne(r *csv.Reader) []string {
	srecord, err := r.Read()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		log.Fatal(err)
	}
	return srecord
}

func readSessionEvent(r *csv.Reader) *sessionEvent {
	record := readOne(r)
	if record == nil {
		return nil
	}
	sess, err := parseSessionEvent(record)
	if err != nil {
		log.Fatal(err)
	}
	return &sess
}

func readChunkEvent(r *csv.Reader) *chunkEvent {
	record := readOne(r)
	if record == nil {
		return nil
	}
	chunk, err := parseChunkEvent(record)
	if err != nil {
		log.Fatal(err)
	}
	return &chunk
}

func readChunkEventFromDB(iter iterator.Iterator) *chunkEvent {
	if !iter.Next() {
		return nil
	}
	reader := bytes.NewReader(iter.Value())
	dec := gob.NewDecoder(reader)
	var c chunkEvent
	err := dec.Decode(&c)
	if err != nil {
		log.Fatal(err)
	}
	return &c
}

type sessionInfo struct {
	lastIndex int
	bps       int
}

func main() {
	var cfgFile, csvDir, cpuprofile string
	var readEventCount int

	flag.StringVar(&cfgFile, "cfg", "cdn-simul.json", "config file")
	flag.StringVar(&csvDir, "csv-dir", ".", "directory includes event csv files")
	flag.IntVar(&readEventCount, "event-count", 0, "event count to process. if 0, process all event")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile")
	flag.Parse()

	if cpuprofile != "" {
		if err := profile.StartCPUProfile(cpuprofile); err != nil {
			log.Fatal(err)
		}
		defer profile.StopCPUProfile()
	}

	f, err := os.OpenFile(cfgFile, os.O_RDONLY, 0755)
	if err != nil {
		log.Fatal(err)
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatal(err)
	}
	cfg := data.Config{}
	if err := json.Unmarshal(b, &cfg); err != nil {
		log.Fatal(err)
	}

	lb, err := lb.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	sessCsvFile := path.Join(csvDir, "session_event.csv")
	chunkCsvFile := path.Join(csvDir, "chunk_event.csv")
	//dbFile := path.Join(csvDir, "chunk.db")

	sfile, err := os.OpenFile(sessCsvFile, os.O_RDONLY, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer sfile.Close()
	cfile, err := os.OpenFile(chunkCsvFile, os.O_RDONLY, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer cfile.Close()
	sreader := csv.NewReader(sfile)
	creader := csv.NewReader(cfile)

	sess := readSessionEvent(sreader)
	chunk := readChunkEvent(creader)

	// db, err := leveldb.OpenFile(dbFile, nil)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// iter := db.NewIterator(nil, nil)
	// chunk := readChunkEventFromDB(iter)

	sessionMap := make(map[string]sessionInfo)
	i := 0
	for {
		i++
		if i == readEventCount {
			break
		}

		if sess == nil || chunk == nil {
			log.Println("completed")
			break
		}

		nextIsSession := true
		if sess.eventTime.After(chunk.EventTime) {
			nextIsSession = false
		}

		if nextIsSession {
			if sess.eventType == sessionCreated {
				evt := data.SessionEvent{
					Time:      sess.eventTime,
					SessionID: sess.sid,
					FileName:  sess.filename,
					Bps:       int64(sess.bandwidth),
				}
				st, err := lb.StartSession(evt)
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("start %s\n", evt.String())
				log.Printf("%v\n", st.String())

				if sess.sid == chunk.SID && chunk.EventTime.Equal(sess.eventTime) {
					evt := data.ChunkEvent{
						Time:      chunk.EventTime.Add(time.Millisecond),
						SessionID: chunk.SID,
						FileName:  chunk.Filename,
						Bps:       int64(sess.bandwidth),
						Index:     int64(chunk.Index),
						ChunkSize: chunkSize,
					}
					st, err := lb.StartChunk(evt)
					if err != nil {
						log.Fatal(err)
					}
					log.Printf("start %s\n", evt.String())
					log.Printf("%v\n", st.String())
				}
				sessionMap[sess.sid] = sessionInfo{lastIndex: chunk.Index, bps: sess.bandwidth}

				sess = readSessionEvent(sreader)
				chunk = readChunkEvent(creader)
				//chunk = readChunkEventFromDB(iter)
			} else { // sessionClosed
				evt := data.ChunkEvent{
					Time:      sess.eventTime.Add(-time.Millisecond),
					SessionID: sess.sid,
					FileName:  sess.filename,
					Bps:       int64(sess.bandwidth),
					Index:     int64(sessionMap[sess.sid].lastIndex),
					ChunkSize: chunkSize,
				}
				st, err := lb.EndChunk(evt)
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("end %s\n", evt.String())
				log.Printf("%v\n", st.String())

				evt2 := data.SessionEvent{
					Time:      sess.eventTime,
					SessionID: sess.sid,
					FileName:  sess.filename,
					Bps:       int64(sess.bandwidth),
				}
				st2, err := lb.EndSession(evt2)
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("end %s\n", evt2.String())
				log.Printf("%v\n", st2.String())

				delete(sessionMap, sess.sid)
				sess = readSessionEvent(sreader)
			}
		} else { // next is chunk
			if chunk.Index == 0 {
			} else {
				evt := data.ChunkEvent{
					Time:      chunk.EventTime.Add(-time.Millisecond),
					SessionID: chunk.SID,
					FileName:  chunk.Filename,
					Bps:       int64(sessionMap[chunk.SID].bps),
					Index:     int64(chunk.Index - 1),
					ChunkSize: chunkSize,
				}
				st, err := lb.EndChunk(evt)
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("end %s\n", evt.String())
				log.Printf("%v\n", st.String())

				evt2 := data.ChunkEvent{
					Time:      chunk.EventTime,
					SessionID: chunk.SID,
					FileName:  chunk.Filename,
					Bps:       int64(sessionMap[chunk.SID].bps),
					Index:     int64(chunk.Index),
					ChunkSize: chunkSize,
				}
				st2, err := lb.StartChunk(evt2)
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("start %s\n", evt2.String())
				log.Printf("%v\n", st2.String())
			}
			sinfo := sessionMap[chunk.SID]
			sinfo.lastIndex = chunk.Index
			sessionMap[chunk.SID] = sinfo

			chunk = readChunkEvent(creader)
			//chunk = readChunkEventFromDB(iter)
		}
	}
}
