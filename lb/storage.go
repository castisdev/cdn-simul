package lb

import (
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"time"

	"github.com/castisdev/cdn-simul/data"
)

type hitRegTSorter []contentHit

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

const defaultSessionDu time.Duration = 10 * time.Minute

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
	var list []contentHit
	for k := range contents {
		if f.curT.Sub(f.fileInfos.Info(k).RegisterT) < 24*time.Hour {
			continue
		}
		v := contentHit{
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
	var list []contentHit
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
			c := contentHit{
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
	var list []contentHit
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
		v := contentHit{
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

// AddDeleter :
type AddDeleter interface {
	Add(fname int)
	Delete(minDelSize int64)
	Exists(fname int) bool
}

// Storage :
type Storage struct {
	fileInfos       *data.FileInfos
	hitRanker       Ranker
	hitRankerForDel Ranker
	contents        map[int]struct{}
	curSize         int64
	limitSize       int64
	pushedT         time.Time
	pushPeriod      time.Duration
	pushDelayN      int // push 배포 시간 = pushPeriod * pushDelayN
	pushingQ        []int
	dawnPushN       int // 03 ~ 09시 push할 컨텐츠 수 배수
	deliverP        *deliverProcessor
	purgeP          *purgeProcessor
}

// NewStorage :
func NewStorage(statDuration, statDurationForDel, shiftPeriod, pushPeriod time.Duration,
	pushDelayN, dawnPushN int, limitSize int64, fi *data.FileInfos,
	initContents []string, delivers []*data.DeliverEvent, purges []*data.PurgeEvent,
	useSessionDuration, useDeleteLru, useFileSize, useTimeWeight bool) *Storage {
	s := &Storage{
		fileInfos:  fi,
		hitRanker:  NewHitRanker(statDuration, shiftPeriod, fi, useSessionDuration, useFileSize, useTimeWeight),
		contents:   make(map[int]struct{}),
		limitSize:  limitSize,
		pushPeriod: pushPeriod,
		pushDelayN: pushDelayN,
		dawnPushN:  dawnPushN,
	}
	if dawnPushN <= 0 {
		s.dawnPushN = 1
	}
	if delivers != nil {
		s.deliverP = &deliverProcessor{events: delivers, fileInfos: fi}
	}
	if purges != nil {
		s.purgeP = &purgeProcessor{events: purges, fileInfos: fi}
	}
	if statDuration != statDurationForDel && statDurationForDel > 0 {
		s.hitRankerForDel = NewHitRanker(statDurationForDel, shiftPeriod, fi, useSessionDuration, useFileSize, useTimeWeight)
	}
	if useDeleteLru {
		s.hitRankerForDel = NewDeleteLruRanker(statDurationForDel, shiftPeriod, fi, useSessionDuration)
	}
	var empty struct{}
	var totalSize int64
	for _, v := range initContents {
		if fi.Exists(v) == false {
			continue
		}
		f := fi.IntName(v)
		if totalSize+fi.Info(f).Size > limitSize {
			break
		}
		totalSize += fi.Info(f).Size
		s.contents[fi.IntName(v)] = empty
	}
	s.curSize = totalSize
	fmt.Printf("new storage statDuration(%v) statDurationForDel(%v) shiftPeriod(%v) useSessionDuration(%v) useDeletLru(%v) useFileSize(%v) useTimeWeight(%v)\n",
		statDuration, statDurationForDel, shiftPeriod, useSessionDuration, useDeleteLru, useFileSize, useTimeWeight)
	return s
}

// UpdateStart :
func (s *Storage) UpdateStart(evt *data.SessionEvent) error {
	s.hitRanker.UpdateStart(evt)
	if s.hitRankerForDel != nil {
		s.hitRankerForDel.UpdateStart(evt)
	}

	if s.pushedT.IsZero() {
		s.pushedT = evt.Time
	} else if evt.Time.Sub(s.pushedT) >= s.pushPeriod {
		s.pushedT = evt.Time
		if err := s.push(evt.Time); err != nil {
			return fmt.Errorf("failed to push, %v", err)
		}
	}
	if s.deliverP != nil {
		s.deliverP.process(evt.Time, s)
	}
	if s.purgeP != nil {
		s.purgeP.process(evt.Time, s.contents)
	}

	return nil
}

// UpdateEnd :
func (s *Storage) UpdateEnd(evt *data.SessionEvent) {
	s.hitRanker.UpdateEnd(evt)
	if s.hitRankerForDel != nil {
		s.hitRankerForDel.UpdateEnd(evt)
	}
}

// Exists :
func (s *Storage) Exists(file int) bool {
	_, ok := s.contents[file]
	return ok
}

// Add :
func (s *Storage) Add(fname int) {
	var empty struct{}
	s.contents[fname] = empty
	s.curSize += s.fileInfos.Info(fname).Size
	fmt.Printf("added %s hitWeight(%d) hitCount(%d)\n",
		s.fileInfos.Info(fname).File, s.hitRanker.Hit(fname), s.hitRanker.HitCount(fname))
}

// Delete :
func (s *Storage) Delete(minDelSize int64) {
	var ranker Ranker
	if s.hitRankerForDel != nil {
		ranker = s.hitRankerForDel
	} else {
		ranker = s.hitRanker
	}
	del := ranker.Deletable(s.contents, minDelSize)

	for _, v := range del {
		delete(s.contents, v)
		s.curSize -= s.fileInfos.Info(v).Size
		fmt.Printf("deleted %s hitWeight(%d) hitCount(%d)\n",
			s.fileInfos.Info(v).File, ranker.Hit(v), ranker.HitCount(v))
	}
}

func (s *Storage) pushStart(v int) {
	s.pushingQ = append(s.pushingQ, v)
}

func (s *Storage) completedPush() (int, error) {
	if len(s.pushingQ) < s.pushDelayN {
		return 0, fmt.Errorf("not exists completed file")
	}
	var v int
	// pushingQ: FIFO (get first item, and shift)
	v, s.pushingQ = s.pushingQ[0], s.pushingQ[1:len(s.pushingQ)]
	return v, nil
}

func (s *Storage) push(t time.Time) error {
	if 3 <= t.Hour() && t.Hour() <= 9 {
		for i := 0; i < s.dawnPushN; i++ {
			err := s.pushOne()
			if err != nil {
				return err
			}
		}
		return nil
	}
	return s.pushOne()
}

func (s *Storage) pushOne() error {
	compl, err := s.completedPush()
	if err == nil {
		s.Add(compl)
	}

	add, rank, err := s.hitRanker.Addable(s.contents, s.limitSize, s.pushingQ)
	if err == ErrNotExistsAddable {
		return nil
	} else if err != nil {
		return err
	}

	delSize := (s.curSize + s.fileInfos.Info(add).Size) - s.limitSize
	if delSize > 0 {
		s.Delete(delSize)
	}

	s.pushStart(add)
	fmt.Printf("add start %s, rank[%d], contentsCount[%d]\n", s.fileInfos.Info(add).File, rank, len(s.contents))

	return nil
}

type deliverProcessor struct {
	events    []*data.DeliverEvent
	curIdx    int
	fileInfos *data.FileInfos
}

func (p *deliverProcessor) process(t time.Time, adder AddDeleter) error {
	for {
		if p.curIdx >= len(p.events) || p.events[p.curIdx].Time.Sub(t) > 0 {
			return nil
		}
		ev := p.events[p.curIdx]

		if p.fileInfos.Exists(ev.FileName) == false {
			p.fileInfos.AddOne(ev.FileName, ev.FileSize, ev.Time)
			fmt.Printf("add %s not exists in flm (deliver event)\n", ev.FileName)
		} else {
			fmt.Printf("add %s (deliver event)\n", ev.FileName)
		}
		f := p.fileInfos.IntName(ev.FileName)
		if adder.Exists(f) {
			fmt.Printf("already exists %s, no deliver\n", ev.FileName)
		} else {
			adder.Delete(p.fileInfos.Info(f).Size)
			adder.Add(f)
		}
		p.curIdx++
		if p.curIdx >= len(p.events) {
			return nil
		}
	}
}

type purgeProcessor struct {
	events    []*data.PurgeEvent
	curIdx    int
	fileInfos *data.FileInfos
}

func (p *purgeProcessor) process(t time.Time, contents map[int]struct{}) error {
	for {
		if p.curIdx >= len(p.events) || p.events[p.curIdx].Time.Sub(t) > 0 {
			return nil
		}
		ev := p.events[p.curIdx]
		if p.fileInfos.Exists(ev.FileName) {
			f := p.fileInfos.IntName(ev.FileName)
			delete(contents, f)
			fmt.Printf("deleted %s (purge event)\n", ev.FileName)
		}
		p.curIdx++
		if p.curIdx >= len(p.events) {
			return nil
		}
	}
}

var layout = "2006-01-02 15:04:05"

// StrToTime :
func StrToTime(str string) time.Time {
	loc, _ := time.LoadLocation("Local")
	t, err := time.ParseInLocation(layout, str, loc)
	if err != nil {
		log.Fatal(err)
	}
	return t
}
