package data

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"
)

// FileInfo :
type FileInfo struct {
	ID        int
	File      string
	Bps       int64
	Size      int64
	RegisterT time.Time
}

// FileInfos :
type FileInfos struct {
	Infos   map[int]*FileInfo // id-info
	Keys    map[string]int    // filename-id
	LastKey int
}

// NewEmptyFileInfos :
func NewEmptyFileInfos() (*FileInfos, error) {
	return NewFileInfos(strings.NewReader(""))
}

// NewFileInfos : csv format [id, filename, bitrate, size, register-time(YYYY-MM-DDTHH:MM:SS)]
// 실제 data는 FLM DB로부터 가져옴
func NewFileInfos(reader io.Reader) (*FileInfos, error) {
	fi := &FileInfos{
		Infos: make(map[int]*FileInfo),
		Keys:  make(map[string]int),
	}
	r := csv.NewReader(reader)

	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read fileinfo file, %v", err)
	}

	for _, v := range records {
		strID := v[0]
		fname := v[1]
		strBps := v[2]
		strSize := v[3]
		strRegT := v[4]

		id, err := strconv.Atoi(strID)
		if err != nil {
			return nil, fmt.Errorf("failed to convert id, %v", err)
		}
		bps, err := strconv.ParseInt(strBps, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert bps, %v", err)
		}
		fsize, err := strconv.ParseInt(strSize, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert size, %v", err)
		}
		t, err := time.Parse("2006-01-02T15:04:05", strRegT)
		if err != nil {
			return nil, fmt.Errorf("failed to convert register time, %v", err)
		}
		fi.Infos[id] = &FileInfo{ID: id, File: fname, Bps: bps, Size: fsize, RegisterT: t}
		fi.Keys[fname] = id
		fi.LastKey = id
	}
	return fi, nil
}

// Exists :
func (f *FileInfos) Exists(fname string) bool {
	_, ok := f.Keys[fname]
	return ok
}

// AddOne :
func (f *FileInfos) AddOne(fname string, fileSize int64, registerT time.Time) {
	keyCandidate := f.LastKey + 1
	for {
		f.LastKey++
		if _, ok := f.Infos[keyCandidate]; !ok {
			f.Keys[fname] = keyCandidate
			f.Infos[keyCandidate] = &FileInfo{ID: keyCandidate, File: fname, Size: fileSize, RegisterT: registerT}
			return
		}
		keyCandidate = f.LastKey + 1
	}
}

// IntName :
func (f *FileInfos) IntName(fname string) int {
	return f.Keys[fname]
}

// Info :
func (f *FileInfos) Info(intName int) *FileInfo {
	if _, ok := f.Infos[intName]; !ok {
		log.Fatalf("not exists file info in flm (int:%v)", intName)
	}
	return f.Infos[intName]
}

// LBHistory :
type LBHistory struct {
	files []string
}

// LoadFromLBHistory :
//
// LB .hitcount.history file example
// historyheader:1498816756
// 160412185439000HD.mpg,1460454952,747400,5626862,172.16.45.13,1,0,0,1=0 0
// 160414101502000HD.mpg,1460596574,747400,5626862,172.16.45.13,1,0,0,1=0 0
func LoadFromLBHistory(filepath string) ([]string, error) {
	b, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read %v, %v", filepath, err)
	}
	str := string(b)
	nlpos := strings.Index(str, "\n")

	r := csv.NewReader(strings.NewReader(str[nlpos:]))

	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read LB History file, %v", err)
	}

	var files []string
	var totalSize int64
	for _, v := range records {
		files = append(files, v[0])
		sz, err := strconv.ParseInt(v[3], 10, 64)
		if err != nil {
			return nil, err
		}
		totalSize += sz
	}
	fmt.Printf("loaded from LB history file, len(%v),size(%v)\n", len(files), totalSize)
	return files, nil
}

func strToTime(str string) time.Time {
	var layout = "2006-01-02 15:04:05.000"
	loc, _ := time.LoadLocation("Local")
	t, _ := time.ParseInLocation(layout, str, loc)
	return t
}

// LoadFromADSAdapterCsv :
// adsadapter.csv format : [deliver-end-time, filename, filesize]
// csv 만들기
//   * adsadapter-log 사용 => adsAdapter.csv
//   * egrep " 49, | 50, | 51, " adsAdapter.csv |cut -d',' -f1,2,3 > adsadapter.csv (노드수가 49/50/51인 것만 선택)
func LoadFromADSAdapterCsv(filepath string) ([]*DeliverEvent, error) {
	b, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read %v, %v", filepath, err)
	}
	r := csv.NewReader(strings.NewReader(string(b)))

	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read ADSAdapter csv file, %v", err)
	}

	var events []*DeliverEvent
	for _, v := range records {
		sz, err := strconv.ParseInt(strings.Trim(v[2], " "), 10, 64)
		if err != nil {
			log.Fatalf("failed to parse %v, %v", filepath, v)
		}
		d := &DeliverEvent{
			Time:     strToTime(strings.Trim(v[0], " ")),
			FileName: strings.Trim(v[1], " "),
			FileSize: sz,
		}
		events = append(events, d)
	}
	fmt.Printf("loaded from %v, count(%v)\n", filepath, len(events))
	return events, nil
}

// LoadFromPurgeCsv :
// purge.csv format : [purge-time,filename]
// grep DeleteFileInClient * -C 2|grep File_Name|awk '{print $2,$10}'|cut -d',' -f3,4,8|sed "s/\"command //"|sed "s/\"//"|sort > purge.csv
func LoadFromPurgeCsv(filepath string) ([]*PurgeEvent, error) {
	b, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read %v, %v", filepath, err)
	}
	r := csv.NewReader(strings.NewReader(string(b)))

	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read purge csv file, %v", err)
	}

	var events []*PurgeEvent
	for _, v := range records {
		events = append(events, &PurgeEvent{Time: strToTime(v[0] + " " + v[1]), FileName: v[2]})
	}
	fmt.Printf("loaded from %v, count(%v)\n", filepath, len(events))
	return events, nil
}
