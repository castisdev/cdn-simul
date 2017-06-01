package glblog

import (
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"strconv"
	"strings"
	"time"
)

// LogFileInfo :
type LogFileInfo struct {
	Fpath string
	Date  time.Time
	Index int
}

func (l LogFileInfo) String() string {
	return fmt.Sprintf("%s, %s, %d", l.Fpath, l.Date.Format(dateLayout), l.Index)
}

// LogFileInfoSorter :
type LogFileInfoSorter []LogFileInfo

func (lis LogFileInfoSorter) Len() int {
	return len(lis)
}
func (lis LogFileInfoSorter) Swap(i, j int) {
	lis[i], lis[j] = lis[j], lis[i]
}
func (lis LogFileInfoSorter) Less(i, j int) bool {
	if lis[i].Date.Equal(lis[j].Date) == false {
		return lis[i].Date.Before(lis[j].Date)
	}
	return lis[i].Index < lis[j].Index
}

// ListLogFiles :
func ListLogFiles(sdir string) []LogFileInfo {
	files, err := ioutil.ReadDir(sdir)
	if err != nil {
		log.Fatal(err, sdir)
	}

	loc, _ := time.LoadLocation("Local")
	var logs []LogFileInfo
	for _, f := range files {
		if f.IsDir() {
			fpath := path.Join(sdir, f.Name())
			logs = append(logs, ListLogFiles(fpath)...)
			continue
		}

		if strings.HasSuffix(f.Name(), "_GLB.log") {
			li := LogFileInfo{Fpath: path.Join(sdir, f.Name())}
			strs := strings.Split(f.Name(), "_")
			if len(strs) != 2 {
				log.Println("invalid filename, ", li.Fpath)
				continue
			}

			if strings.Contains(f.Name(), "[") {
				strs2 := strings.FieldsFunc(strs[0], func(c rune) bool {
					return c == '[' || c == ']'
				})
				li.Date, err = time.ParseInLocation(dateLayout, strs2[0], loc)
				if err != nil {
					log.Println("invalid filename, ", li.Fpath, err)
					continue
				}
				li.Index, err = strconv.Atoi(strs2[1])
				if err != nil {
					log.Println("invalid filename, ", li.Fpath, err)
					continue
				}
			} else {
				li.Date, err = time.ParseInLocation(dateLayout, strs[0], loc)
				if err != nil {
					log.Println("invalid filename, ", li.Fpath, err)
					continue
				}
				li.Index = 0
			}

			logs = append(logs, li)
		}
	}

	return logs
}
