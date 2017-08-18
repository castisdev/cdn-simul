package lb

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/castisdev/cdn-simul/data"
)

// HitInfo :
type HitInfo struct {
	filename int
	hit      int64
	filesize int64
	regT     time.Time
}

func (c HitInfo) String() string {
	return fmt.Sprintf("%v %v %v %v", c.filename, c.hit, c.filesize, c.regT)
}

type hitRegTSorter []HitInfo

func (chs hitRegTSorter) Len() int {
	return len(chs)
}
func (chs hitRegTSorter) Swap(i, j int) {
	chs[i], chs[j] = chs[j], chs[i]
}
func (chs hitRegTSorter) Less(i, j int) bool {
	return chs[i].hit > chs[j].hit || (chs[i].hit == chs[j].hit && chs[i].regT.After(chs[j].regT))
}

// Ranker :
type Ranker interface {
	UpdateStart(evt *data.SessionEvent)
	UpdateEnd(evt *data.SessionEvent)
	Addable(contents map[int]struct{}, storageSize int64, exclude []int) (id, rank int, err error)
	Deletable(contents map[int]struct{}, minDelSize int64) []int
	Hit(fname int) int64
	HitCount(fname int) int64
	HitList(exclude []int) []HitInfo
}

// HitRanker :
type HitRanker struct {
	fileInfos          *data.FileInfos
	shiftPeriod        time.Duration
	shiftT             time.Time
	contentHits        []map[int]int64 // slot(shift 시간)별 content hit weight 관리
	contentHitCounts   []map[int]int64 // slot(shift 시간)별 content hit count 관리
	curT               time.Time
	useSessionDuration bool
	useFileSize        bool
	useTimeWeight      bool
}

// NewHitRanker :
func NewHitRanker(statDuration, shiftPeriod time.Duration, fi *data.FileInfos, useSessionDuration, useFileSize, useTimeWeight bool) *HitRanker {
	hitSlotSize := int(statDuration.Minutes() / shiftPeriod.Minutes())
	f := &HitRanker{
		fileInfos:          fi,
		shiftPeriod:        shiftPeriod,
		contentHits:        make([]map[int]int64, hitSlotSize),
		contentHitCounts:   make([]map[int]int64, hitSlotSize),
		useSessionDuration: useSessionDuration,
		useFileSize:        useFileSize,
		useTimeWeight:      useTimeWeight,
	}
	if useFileSize {
		f.useSessionDuration = true
	}
	for i := 0; i < hitSlotSize; i++ {
		f.contentHits[i] = make(map[int]int64)
		f.contentHitCounts[i] = make(map[int]int64)
	}
	return f
}

// UpdateStart :
func (f *HitRanker) UpdateStart(evt *data.SessionEvent) {
	if f.shiftT.IsZero() {
		f.shiftT = evt.Time
	} else if evt.Time.Sub(f.shiftT) >= f.shiftPeriod {
		f.shift(evt.Time)
		f.shiftT = evt.Time
	}

	curIdx := len(f.contentHits) - 1
	if f.useSessionDuration == false {
		f.contentHits[curIdx][evt.IntFileName] += f.hitWeight(evt)
	}
	f.contentHitCounts[curIdx][evt.IntFileName]++
	f.curT = evt.Time
}

// UpdateEnd :
func (f *HitRanker) UpdateEnd(evt *data.SessionEvent) {
	if f.useSessionDuration {
		curIdx := len(f.contentHits) - 1
		if f.useFileSize {
			sz := f.fileInfos.Info(evt.IntFileName).Size
			if sz == 0 {
				sz = 1
			}
			f.contentHits[curIdx][evt.IntFileName] += int64(f.hitWeight(evt) * int64(evt.Duration.Seconds()) / sz)
		} else {
			f.contentHits[curIdx][evt.IntFileName] += f.hitWeight(evt) * int64(evt.Duration.Seconds())
		}
	}
}

func (f *HitRanker) hitWeight(evt *data.SessionEvent) int64 {
	if f.useFileSize {
		return evt.Bps
	}
	// bps는 100Kbps보다 크다고 가정
	return int64(evt.Bps / 100000)
}

// Deletable :
func (f *HitRanker) Deletable(contents map[int]struct{}, minDelSize int64) []int {
	var list []HitInfo
	for k := range contents {
		if f.curT.Sub(f.fileInfos.Info(k).RegisterT) < 24*time.Hour {
			continue
		}
		v := HitInfo{
			filename: k,
			hit:      f.Hit(k),
			filesize: f.fileInfos.Info(k).Size,
			regT:     f.fileInfos.Info(k).RegisterT,
		}
		list = append(list, v)
	}
	sort.Sort(hitRegTSorter(list))

	var ret []int
	var totalSize int64
	for i := len(list) - 1; i >= 0; i-- {
		if totalSize >= minDelSize {
			return ret
		}
		ret = append(ret, list[i].filename)
		totalSize += list[i].filesize
	}
	return ret
}

// ErrNotExistsAddable :
var ErrNotExistsAddable = errors.New("not exists addable file")

// Addable :
func (f *HitRanker) Addable(contents map[int]struct{}, storageSize int64, exclude []int) (id, rank int, err error) {
	list := f.HitList(exclude)
	id = -1
	var totalSize int64
	for i, v := range list {
		totalSize += v.filesize
		if storageSize < totalSize {
			break
		}
		if _, ok := contents[v.filename]; !ok {
			id = v.filename
			rank = i
			break
		}
	}
	if id == -1 {
		return 0, 0, ErrNotExistsAddable
	}
	return
}

// Hit :
func (f *HitRanker) Hit(fname int) int64 {
	sum := int64(0)
	slotN := len(f.contentHits)
	x := 0.9
	for i := 0; i < slotN; i++ {
		if f.useTimeWeight {
			// slot1..slotN 은 시간순
			// hit-weight = slot1 + slot2 * (x**1) + slot3 * (x**2) ...
			sum += f.contentHits[i][fname] * int64(math.Pow(float64(x), float64(slotN-i-1)))
		} else {
			sum += f.contentHits[i][fname]
		}
	}
	// 입수된지 얼마 안된 컨텐츠의 빈 슬롯은 평균값으로 보정
	emptySlotN := int64(slotN) - int64(f.curT.Sub(f.fileInfos.Info(fname).RegisterT)/f.shiftPeriod)
	if 0 < emptySlotN && emptySlotN < int64(slotN) {
		adjust := (sum * emptySlotN) / (int64(slotN) - emptySlotN)
		sum += adjust
	}
	return sum
}

// HitCount :
func (f *HitRanker) HitCount(fname int) int64 {
	sum := int64(0)
	for i := 0; i < len(f.contentHitCounts); i++ {
		sum += f.contentHitCounts[i][fname]
	}
	return sum
}

// HitList :
func (f *HitRanker) HitList(exclude []int) []HitInfo {
	var list []HitInfo
	added := make(map[int]struct{})
	var empty struct{}
	for _, v := range exclude {
		added[v] = empty
	}

	for _, v := range f.contentHits {
		for k := range v {
			if _, ok := added[k]; ok {
				continue
			}
			c := HitInfo{
				filename: k,
				hit:      f.Hit(k),
				filesize: f.fileInfos.Info(k).Size,
				regT:     f.fileInfos.Info(k).RegisterT,
			}
			list = append(list, c)
			added[k] = empty
		}
	}

	sort.Sort(hitRegTSorter(list))
	return list
}

func (f *HitRanker) shift(t time.Time) {
	slotN := len(f.contentHits)
	shiftN := int(t.Sub(f.shiftT).Minutes() / f.shiftPeriod.Minutes())
	if shiftN >= slotN {
		for i := 0; i < slotN; i++ {
			f.contentHits[i] = make(map[int]int64)
			f.contentHitCounts[i] = make(map[int]int64)
		}
		return
	}

	for i := 0; i < slotN-shiftN; i++ {
		f.contentHits[i] = f.contentHits[shiftN+i]
		f.contentHitCounts[i] = f.contentHitCounts[shiftN+i]
	}
	for i := 0; i < shiftN; i++ {
		f.contentHits[slotN-i-1] = make(map[int]int64)
		f.contentHitCounts[slotN-i-1] = make(map[int]int64)
	}
}

// DeleteLruRanker :
type DeleteLruRanker struct {
	hitRanker      *HitRanker
	recentSessionT map[int]time.Time
}

// NewDeleteLruRanker :
func NewDeleteLruRanker(statDuration, shiftPeriod time.Duration, fi *data.FileInfos, useSessionDuration bool) *DeleteLruRanker {
	f := &DeleteLruRanker{
		hitRanker:      NewHitRanker(statDuration, shiftPeriod, fi, useSessionDuration, false, false),
		recentSessionT: make(map[int]time.Time),
	}
	return f
}

// UpdateStart :
func (f *DeleteLruRanker) UpdateStart(evt *data.SessionEvent) {
	f.hitRanker.UpdateStart(evt)
	f.recentSessionT[evt.IntFileName] = evt.Time
}

// UpdateEnd :
func (f *DeleteLruRanker) UpdateEnd(evt *data.SessionEvent) {
	f.hitRanker.UpdateEnd(evt)
}

// Deletable :
func (f *DeleteLruRanker) Deletable(contents map[int]struct{}, minDelSize int64) []int {
	var list []HitInfo
	for k := range contents {
		if f.hitRanker.curT.Sub(f.hitRanker.fileInfos.Info(k).RegisterT) < 24*time.Hour {
			if _, ok := f.recentSessionT[k]; !ok {
				// 초기 배포의 최근 세션시간을 오래전 임의의 시간으로 설정하여 hit가 없으면 바로 삭제될 수 있도록 처리
				f.recentSessionT[k] = StrToTime("2001-01-01 00:00:00")
			}
			continue
		}
		sessT, ok := f.recentSessionT[k]
		if !ok {
			sessT = f.hitRanker.fileInfos.Info(k).RegisterT
		}
		v := HitInfo{
			filename: k,
			hit:      f.hitRanker.Hit(k),
			filesize: f.hitRanker.fileInfos.Info(k).Size,
			regT:     sessT,
		}
		list = append(list, v)
	}
	sort.Sort(hitRegTSorter(list))

	var ret []int
	var totalSize int64
	for i := len(list) - 1; i >= 0; i-- {
		if totalSize >= minDelSize {
			return ret
		}
		ret = append(ret, list[i].filename)
		totalSize += list[i].filesize
	}
	return ret
}

// Addable :
func (f *DeleteLruRanker) Addable(contents map[int]struct{}, storageSize int64, exclude []int) (id, rank int, err error) {
	return f.hitRanker.Addable(contents, storageSize, exclude)
}

// Hit :
func (f *DeleteLruRanker) Hit(fname int) int64 {
	return f.hitRanker.Hit(fname)
}

// HitCount :
func (f *DeleteLruRanker) HitCount(fname int) int64 {
	return f.hitRanker.HitCount(fname)
}

// HitList :
func (f *DeleteLruRanker) HitList(exclude []int) []HitInfo {
	return f.hitRanker.HitList(exclude)
}
