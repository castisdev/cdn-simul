package lb

import (
	"errors"
	"fmt"
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

// HitRanker :
type HitRanker struct {
	fileInfos          *data.FileInfos
	shiftPeriod        time.Duration
	shiftT             time.Time
	contentHits        []map[int]int64 // slot(shift 시간)별 content hit 관리
	curT               time.Time
	useSessionDuration bool
}

// NewHitRanker :
func NewHitRanker(statDuration, shiftPeriod time.Duration, fi *data.FileInfos, useSessionDuration bool) *HitRanker {
	hitSlotSize := int(statDuration.Minutes() / shiftPeriod.Minutes())
	f := &HitRanker{
		fileInfos:          fi,
		shiftPeriod:        shiftPeriod,
		contentHits:        make([]map[int]int64, hitSlotSize),
		useSessionDuration: useSessionDuration,
	}
	for i := 0; i < hitSlotSize; i++ {
		f.contentHits[i] = make(map[int]int64)
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
	if f.useSessionDuration {
		f.contentHits[curIdx][evt.IntFileName] += f.hitWeight(evt) * int64(evt.Duration.Seconds())
	} else {
		f.contentHits[curIdx][evt.IntFileName] += f.hitWeight(evt)
	}
	f.curT = evt.Time
}

// UpdateEnd :
func (f *HitRanker) UpdateEnd(evt *data.SessionEvent) {
}

func (f *HitRanker) hitWeight(evt *data.SessionEvent) int64 {
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
			hit:      f.hit(k),
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
				hit:      f.hit(k),
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

func (f *HitRanker) hit(fname int) int64 {
	sum := int64(0)
	for i := 0; i < len(f.contentHits); i++ {
		sum += f.contentHits[i][fname]
	}
	return sum
}

func (f *HitRanker) shift(t time.Time) {
	slotN := len(f.contentHits)
	shiftN := int(t.Sub(f.shiftT).Minutes() / f.shiftPeriod.Minutes())
	if shiftN >= slotN {
		for i := 0; i < slotN; i++ {
			f.contentHits[i] = make(map[int]int64)
		}
		return
	}

	for i := 0; i < slotN-shiftN; i++ {
		f.contentHits[i] = f.contentHits[shiftN+i]
	}
	for i := 0; i < shiftN; i++ {
		f.contentHits[slotN-i-1] = make(map[int]int64)
	}
}

// AddDeleter :
type AddDeleter interface {
	Add(fname int)
	Delete(minDelSize int64)
}

// Storage :
type Storage struct {
	fileInfos       *data.FileInfos
	hitRanker       *HitRanker
	hitRankerForDel *HitRanker
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
	initContents []string, delivers []*data.DeliverEvent, purges []*data.PurgeEvent, useSessionDuration bool) *Storage {
	s := &Storage{
		fileInfos:  fi,
		hitRanker:  NewHitRanker(statDuration, shiftPeriod, fi, useSessionDuration),
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
		s.hitRankerForDel = NewHitRanker(statDurationForDel, shiftPeriod, fi, useSessionDuration)
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
	fmt.Printf("new storage statDuration(%v) statDurationForDel(%v) shiftPeriod(%v) useSessionDuration(%v)\n",
		statDuration, statDurationForDel, shiftPeriod, useSessionDuration)
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
	fmt.Printf("added %s\n", s.fileInfos.Info(fname).File)
}

// Delete :
func (s *Storage) Delete(minDelSize int64) {
	var del []int
	if s.hitRankerForDel != nil {
		del = s.hitRankerForDel.Deletable(s.contents, minDelSize)
	} else {
		del = s.hitRanker.Deletable(s.contents, minDelSize)
	}
	for _, v := range del {
		delete(s.contents, v)
		s.curSize -= s.fileInfos.Info(v).Size
		fmt.Printf("deleted %s\n", s.fileInfos.Info(v).File)
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
			GB := int64(1024 * 1024 * 1024)
			p.fileInfos.AddOne(ev.FileName, 2*GB, ev.Time)
			fmt.Printf("add %s unknown filesize (deliver event)\n", ev.FileName)
		} else {
			fmt.Printf("add %s (deliver event)\n", ev.FileName)
		}
		f := p.fileInfos.IntName(ev.FileName)
		adder.Delete(p.fileInfos.Info(f).Size)
		adder.Add(f)
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
