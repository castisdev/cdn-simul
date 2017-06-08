package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/castisdev/cdn-simul/glblog"
)

func main() {
	sdir := flag.String("sdir", "", "source directory")
	odir := flag.String("odir", "filtered", "source directory")
	isCenter := flag.Bool("center", false, "center glb log")
	flag.Parse()

	os.MkdirAll(*odir, 0777)
	files := glblog.ListLogFiles(*sdir)
	sort.Sort(glblog.LogFileInfoSorter(files))

	sidMap := map[string]struct{}{}
	for _, lfi := range files {
		doOneFile(lfi, *odir, *isCenter, sidMap)
	}
}

var dateLayout = "2006-01-02"

func doOneFile(lfi glblog.LogFileInfo, odir string, isCenter bool, sidMap map[string]struct{}) {
	if isCenter {
		dir := filepath.Dir(lfi.Fpath)
		dir = filepath.Dir(dir)
		ctCode := filepath.Base(dir)
		odir = path.Join(odir, ctCode)
		os.MkdirAll(odir, 0777)
	}

	f, err := os.Open(lfi.Fpath)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	outFilename := filepath.Join(odir, lfi.Date.Format(dateLayout)+"_GLB.log")
	of, err := os.OpenFile(outFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println(err)
		return
	}
	defer of.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if strings.Contains(line, "cueTone") {
			continue
		}

		if strings.Contains(line, "Successfully New Setup Session") == false &&
			strings.Contains(line, "Successfully New SemiSetup Session") == false &&
			strings.Contains(line, "OnTeardownNotification") == false {
			continue
		}

		if isCenter {
			if strings.Contains(line, "Successfully New Setup Session") ||
				strings.Contains(line, "Successfully New SemiSetup Session") {
				strs := strings.SplitN(line, ",", 8)
				logLine := strings.Trim(strs[7], `"`)

				sepFunc := func(c rune) bool {
					return c == '[' || c == ']' || c == ',' || c == ' '
				}

				idx := strings.Index(logLine, "SessionId")
				if idx == -1 {
					continue
				}
				strs2 := strings.FieldsFunc(logLine[idx:], sepFunc)
				sid := strs2[1]

				idx = strings.Index(logLine, "RequestURL")
				if idx == -1 {
					continue
				}
				strs2 = strings.FieldsFunc(logLine[idx:], sepFunc)
				url := strs2[1]

				idx = strings.Index(url, "p=")
				if idx == -1 {
					continue
				}
				strs2 = strings.Split(url[idx+2:], ":")
				if len(strs2) < 4 {
					continue
				}
				dongCode := strs2[3]

				found := false
				for _, d := range dongCodes {
					if d == dongCode {
						found = true
						break
					}
				}

				if found == false {
					continue
				}

				sidMap[sid] = struct{}{}
			} else if strings.Contains(line, "OnTeardownNotification") {
				strs := strings.SplitN(line, ",", 8)
				logLine := strings.Trim(strs[7], `"`)
				strs2 := strings.Split(logLine, ",")
				if len(strs2) < 2 {
					continue
				}
				sid := strings.TrimSpace(strs2[1])

				if _, exists := sidMap[sid]; exists {
					delete(sidMap, sid)
				} else {
					continue
				}
			}
		}

		fmt.Fprintln(of, line)
	}

	if err := s.Err(); err != nil {
		log.Println(err)
	}

	log.Println("done with ", lfi.Fpath)
}

var dongCodes = []string{"303249", "303203", "303242", "303202", "303231", "303209", "305842", "303204", "303244", "305826", "303253",
	"303252", "303260", "303217", "305827", "305817", "303246", "303238", "303257", "305830", "303254", "303250", "305836", "302833",
	"303223", "303216", "303258", "303233", "303206", "303241", "303042", "303205", "303230", "303247", "303237", "303218", "303020",
	"303236", "305824", "303220", "303228", "303227", "303207", "303226", "305828", "305843", "303225", "305819", "303255", "305818",
	"303232", "303240", "303208", "305820", "305822", "303259", "303229", "303221", "303235", "305829", "305801", "851316", "303251",
	"303245", "303256", "303219", "303222", "303239", "303224", "303214", "305807", "305821", "303243", "303234"}
