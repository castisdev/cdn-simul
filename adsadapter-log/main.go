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
	"time"

	"github.com/castisdev/cdn-simul/loginfo"
)

var dateLayout = "2006-01-02"
var layout = "2006-01-02 15:04:05.000"

var suffix = "adsAdapter"
var fileSuffix = "_adsAdapter.log"
var logDir = "log-filtered"

func main() {
	sdir := flag.String("sdir", "", "source directory")
	odir := flag.String("odir", "", "output directory")
	flag.Parse()

	files := loginfo.ListLogFiles(*sdir, suffix)
	sort.Sort(loginfo.LogFileInfoSorter(files))

	for _, lfi := range files {
		outFilename := filepath.Join(*odir, logDir, lfi.Date.Format(dateLayout)+fileSuffix)
		os.RemoveAll(outFilename)
	}

	csvFilename := filepath.Join(*odir, suffix+".csv")
	os.MkdirAll(filepath.Dir(csvFilename), 0777)
	csvf, err := os.Create(csvFilename)
	if err != nil {
		log.Println(err)
		return
	}
	defer csvf.Close()

	for _, lfi := range files {
		doOneFile(lfi, *odir, csvf)
	}

	if lastInfo != nil {
		fmt.Fprintln(csvf, *lastInfo)
	}
}

type adapterInfo struct {
	started       time.Time
	priority      string
	multicastIP   string
	multicastPort string
	filename      string
	filesize      string
	serverDir     string
	clientDir     string
	cpCode        string
	nodes         nodeList
}

func (i adapterInfo) String() string {
	return fmt.Sprintf("%s, %38s, %11s, %3s, %s, %s, %12s, %16s, %s, %2d, %v",
		i.started.Format(layout), i.filename, i.filesize, i.priority, i.multicastIP, i.multicastPort, i.serverDir, i.clientDir, i.cpCode, len(i.nodes), i.nodes)
}

type nodeList []string

func (nl nodeList) String() string {
	str := `"`
	for idx, n := range nl {
		str += n
		if idx != len(nl)-1 {
			str += ", "
		}
		if idx > 3 {
			str += "..."
			break
		}
	}
	str += `"`
	return str
}

var lastInfo *adapterInfo

func doOneFile(lfi loginfo.LogFileInfo, odir string, csvf *os.File) {
	loc, _ := time.LoadLocation("Local")

	f, err := os.Open(lfi.Fpath)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	outFilename := filepath.Join(odir, logDir, lfi.Date.Format(dateLayout)+fileSuffix)
	os.MkdirAll(filepath.Dir(outFilename), 0777)
	of, err := os.OpenFile(outFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println(err)
		return
	}
	defer of.Close()

	// sepFunc := func(c rune) bool {
	// 	return c == '[' || c == ']' || c == ',' || c == ' ' || c == ':'
	// }

	// command data name : Transfer_Mode_Priority, command value : 999

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()

		if strings.HasSuffix(line, "command type : FileTransfer") == false &&
			strings.Contains(line, "command data name : Transfer_Mode_Priority") == false &&
			strings.Contains(line, "command data name : Multicast_Channel_IP") == false &&
			strings.Contains(line, "command data name : Multicast_Channel_Port") == false &&
			strings.Contains(line, "command data name : File_Name") == false &&
			strings.Contains(line, "command data name : File_Size") == false &&
			strings.Contains(line, "command data name : Server_Directory") == false &&
			strings.Contains(line, "command data name : Client_Directory") == false &&
			strings.Contains(line, "node info adcIP : ") == false &&
			strings.Contains(line, ",InsertSchedule,") == false &&
			strings.Contains(line, "CommandFileTransfer,,TRANSACTION ID : ") == false &&
			strings.Contains(line, "send transfer notification success TRANSACTION ID : ") == false {
			continue
		}

		fmt.Fprintln(of, line)

		strs := strings.SplitN(line, ",", 8)
		strStarted := strs[2] + " " + strs[3]
		logLine := strings.Trim(strs[7], `"`)

		if strings.HasSuffix(line, "command type : FileTransfer") {
			if lastInfo != nil {
				sort.Strings(lastInfo.nodes)
				fmt.Fprintln(csvf, *lastInfo)
			}
			lastInfo = &adapterInfo{}
			lastInfo.started, _ = time.ParseInLocation(layout, strStarted, loc)
		} else if strings.Contains(line, "command data name : Transfer_Mode_Priority") {
			idx := strings.Index(logLine, "command value")
			fmt.Sscanf(logLine[idx:], "command value : %s", &lastInfo.priority)
		} else if strings.Contains(line, "command data name : Multicast_Channel_IP") {
			idx := strings.Index(logLine, "command value")
			fmt.Sscanf(logLine[idx:], "command value : %s", &lastInfo.multicastIP)
		} else if strings.Contains(line, "command data name : Multicast_Channel_Port") {
			idx := strings.Index(logLine, "command value")
			fmt.Sscanf(logLine[idx:], "command value : %s", &lastInfo.multicastPort)
		} else if strings.Contains(line, "command data name : File_Name") {
			idx := strings.Index(logLine, "command value")
			fmt.Sscanf(logLine[idx:], "command value : %s", &lastInfo.filename)
			lastInfo.cpCode = lastInfo.filename[1:3]
		} else if strings.Contains(line, "command data name : File_Size") {
			idx := strings.Index(logLine, "command value")
			fmt.Sscanf(logLine[idx:], "command value : %s", &lastInfo.filesize)
		} else if strings.Contains(line, "command data name : Server_Directory") {
			idx := strings.Index(logLine, "command value")
			fmt.Sscanf(logLine[idx:], "command value : %s", &lastInfo.serverDir)
		} else if strings.Contains(line, "command data name : Client_Directory") {
			idx := strings.Index(logLine, "command value")
			fmt.Sscanf(logLine[idx:], "command value : %s", &lastInfo.clientDir)
		} else if strings.Contains(line, "node info adcIP : ") {
			// node info adcIP : 125.144.161.3, lsmIP : 125.144.161.5
			idx := strings.Index(logLine, "lsmIP")
			var lsmIP string
			fmt.Sscanf(logLine[idx:], "lsmIP : %s", &lsmIP)
			lastInfo.nodes = append(lastInfo.nodes, lsmIP)
		}
	}
}
