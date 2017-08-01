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

	"github.com/castisdev/cdn-simul/loginfo"
)

func main() {
	sdir := flag.String("sdir", "", "source directory")
	odir := flag.String("odir", "filtered", "source directory")
	isCenter := flag.Bool("center", false, "center glb log")
	loc := flag.String("loc", "GB", "location name, (GB | NIC)")
	flag.Parse()

	os.MkdirAll(*odir, 0777)
	files := loginfo.ListLogFiles(*sdir, "GLB")
	sort.Sort(loginfo.LogFileInfoSorter(files))

	dongCodes := gangbukDongCodes
	if *loc == "NIC" {
		dongCodes = namincheonDongCodes
	}
	sidMap := map[string]struct{}{}
	for _, lfi := range files {
		doOneFile(lfi, *odir, *isCenter, dongCodes, sidMap)
	}
}

var dateLayout = "2006-01-02"

func doOneFile(lfi loginfo.LogFileInfo, odir string, isCenter bool, dongCodes []string, sidMap map[string]struct{}) {
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
		if strings.Contains(line, "cueTone") || strings.Contains(line, "test1.mpg") {
			continue
		}

		if strings.Contains(line, "Successfully New Setup Session") == false &&
			strings.Contains(line, "Successfully New SemiSetup Session") == false &&
			strings.Contains(line, "OnTeardownNotification") == false &&
			strings.Contains(line, "result is file not found") == false {
			continue
		}

		if strings.Contains(line, "OnDescribeResponse") {
			continue
		}

		if isCenter {
			if strings.Contains(line, "result is file not found") {
				continue
			}

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

var gangbukDongCodes = []string{"303249", "303203", "303242", "303202", "303231", "303209", "305842", "303204", "303244", "305826", "303253",
	"303252", "303260", "303217", "305827", "305817", "303246", "303238", "303257", "305830", "303254", "303250", "305836", "302833",
	"303223", "303216", "303258", "303233", "303206", "303241", "303042", "303205", "303230", "303247", "303237", "303218", "303020",
	"303236", "305824", "303220", "303228", "303227", "303207", "303226", "305828", "305843", "303225", "305819", "303255", "305818",
	"303232", "303240", "303208", "305820", "305822", "303259", "303229", "303221", "303235", "305829", "305801", "851316", "303251",
	"303245", "303256", "303219", "303222", "303239", "303224", "303214", "305807", "305821", "303243", "303234"}

var namincheonDongCodes = []string{
	"337174", "335040", "337178", "365290", "337183", "337158", "337192", "334041", "337131", "365292", "337146", "800219", "335038",
	"335034", "365305", "365207", "337166", "337137", "335014", "337136", "365294", "337169", "337186", "337164", "335020", "335010",
	"337172", "335055", "335033", "337122", "365296", "335043", "337120", "365259", "335001", "365251", "337180", "365230", "337185",
	"337190", "337108", "365214", "335035", "335039", "337173", "335049", "331059", "365291", "337152", "337153", "335017", "365255",
	"365242", "365235", "365287", "335051", "365308", "337177", "337195", "331061", "337175", "365257", "337117", "337101", "331058",
	"365245", "331062", "337163", "337130", "365202", "337188", "337168", "337159", "337181", "365289", "337147", "335023", "335006",
	"337167", "337197", "337123", "365285", "335053", "337144", "337004", "337179", "365307", "337124", "337127", "365258", "337001",
	"335037", "337156", "365252", "337160", "365244", "365233", "335050", "337138", "365250", "365238", "335052", "337110", "337140",
	"337149", "365247", "365246", "365261", "365248", "365212", "335032", "337002", "365260", "337111", "365237", "365232", "337142",
	"365206", "365213", "365236", "335042", "365262", "365249", "365256", "337176", "337145", "337191", "337128", "337171", "365288",
	"337196", "337129", "337161", "365211", "335031", "365234", "365205", "337105", "365264", "337141", "337135", "335036", "365240",
	"337134", "365204", "337106", "337114", "337189", "337125", "337170", "365210", "365302", "331055", "337112", "337104", "337155",
	"365306", "337126", "365286", "335030", "365243", "337121", "365309", "337003", "337193", "335056", "365263", "337119", "337118",
	"337194", "337184", "337107", "365293", "337133", "337162", "337187", "337102", "337109", "337116", "337115", "365301", "365231",
	"337165", "365295", "337150", "365304", "337132", "337151", "365201", "337143", "337113", "365203", "337148", "337139", "337157",
	"331057", "335054", "365241", "331060", "337103", "365303", "331028", "333003", "333021", "336037", "333053", "331035", "332004",
	"333029", "331040", "331009", "331036", "333038", "332018", "337014", "337026", "331014", "336035", "331012", "332016", "333037",
	"336036", "332005", "333013", "337011", "336027", "336020", "332003", "333018", "336025", "333015", "332002", "333041", "333022",
	"337015", "336023", "331019", "332019", "331048", "331023", "333048", "331027", "800208", "332007", "335041", "331047", "333008",
	"332008", "331017", "336057", "337024", "331006", "331001", "336032", "851474", "333011", "331039", "336052", "331026", "333023",
	"332011", "337037", "331029", "331031", "333035", "331030", "333016", "337018", "333030", "336022", "336061", "333061", "337035",
	"336006", "336019", "333050", "336026", "337034", "337036", "361728", "333047", "336033", "336040", "333052", "337017", "337009",
	"335027", "331037", "331015", "332017", "331007", "331013", "336028", "333042", "332012", "361726", "337016", "331003", "331041",
	"331038", "332015", "336038", "333043", "331024", "361725", "331034", "337005", "331044", "333040", "332013", "337010", "336034",
	"337013", "331045", "333031", "333036", "331033", "331004", "333006", "361727", "333049", "332010", "333044", "333005", "337007",
	"336065", "336048", "337039", "337006", "337008", "331016", "337019", "331032", "333063", "336021", "337033", "331005", "333051",
	"331042", "333046", "336029", "331025", "333017", "331018", "337038", "332006", "332014", "331046", "336044", "333039", "333007",
	"337025", "331051", "331052", "331010", "333062", "332023", "331056", "333009", "331050", "361724", "333045", "332020", "331008",
}
