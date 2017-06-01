package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/castisdev/cdn-simul/glblog"
)

func main() {
	sdir := flag.String("sdir", "", "source directory")
	odir := flag.String("odir", "filtered", "source directory")
	flag.Parse()

	os.MkdirAll(*odir, 0777)
	files := glblog.ListLogFiles(*sdir)
	sort.Sort(glblog.LogFileInfoSorter(files))

	for _, lfi := range files {
		doOneFile(lfi, *odir)
	}
}

var dateLayout = "2006-01-02"

func doOneFile(lfi glblog.LogFileInfo, odir string) {
	f, err := os.Open(lfi.Fpath)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	of, err := os.OpenFile(filepath.Join(odir, lfi.Date.Format(dateLayout)+"_GLB.log"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
			strings.Contains(line, "OnRetrieveBandwidthResponse") == false &&
			strings.Contains(line, "OnTeardownNotification") == false {
			continue
		}

		fmt.Fprintln(of, line)
	}

	if err := s.Err(); err != nil {
		log.Println(err)
	}

	log.Println("done with ", lfi.Fpath)
}
