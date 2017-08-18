package lb

import (
	"fmt"
	"log"
	"time"

	"github.com/castisdev/cdn-simul/data"
)

// AddDeleter :
type AddDeleter interface {
	Add(fname int)
	Delete(minDelSize int64)
	Exists(fname int) bool
}

// Storage :
type Storage interface {
	UpdateStart(evt *data.SessionEvent) error
	UpdateEnd(evt *data.SessionEvent)
	Exists(file int) bool
	LimitSize() int64
}

// FilebaseStorage :
type FilebaseStorage struct {
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

// NewFilebaseStorage :
func NewFilebaseStorage(statDuration, statDurationForDel, shiftPeriod, pushPeriod time.Duration,
	pushDelayN, dawnPushN int, limitSize int64, fi *data.FileInfos,
	initContents []string, delivers []*data.DeliverEvent, purges []*data.PurgeEvent,
	useSessionDuration, useDeleteLru, useFileSize, useTimeWeight bool) *FilebaseStorage {
	s := &FilebaseStorage{
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
	fmt.Printf("new filebase storage statDuration(%v) statDurationForDel(%v) shiftPeriod(%v) useSessionDuration(%v) useDeletLru(%v) useFileSize(%v) useTimeWeight(%v)\n",
		statDuration, statDurationForDel, shiftPeriod, useSessionDuration, useDeleteLru, useFileSize, useTimeWeight)
	return s
}

// UpdateStart :
func (s *FilebaseStorage) UpdateStart(evt *data.SessionEvent) error {
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
func (s *FilebaseStorage) UpdateEnd(evt *data.SessionEvent) {
	s.hitRanker.UpdateEnd(evt)
	if s.hitRankerForDel != nil {
		s.hitRankerForDel.UpdateEnd(evt)
	}
}

// Exists :
func (s *FilebaseStorage) Exists(file int) bool {
	_, ok := s.contents[file]
	return ok
}

// LimitSize :
func (s *FilebaseStorage) LimitSize() int64 {
	return s.limitSize
}

// Add :
func (s *FilebaseStorage) Add(fname int) {
	var empty struct{}
	s.contents[fname] = empty
	s.curSize += s.fileInfos.Info(fname).Size
	fmt.Printf("added %s hitWeight(%d) hitCount(%d)\n",
		s.fileInfos.Info(fname).File, s.hitRanker.Hit(fname), s.hitRanker.HitCount(fname))
}

// Delete :
func (s *FilebaseStorage) Delete(minDelSize int64) {
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

func (s *FilebaseStorage) pushStart(v int) {
	s.pushingQ = append(s.pushingQ, v)
}

func (s *FilebaseStorage) completedPush() (int, error) {
	if len(s.pushingQ) < s.pushDelayN {
		return 0, fmt.Errorf("not exists completed file")
	}
	var v int
	// pushingQ: FIFO (get first item, and shift)
	v, s.pushingQ = s.pushingQ[0], s.pushingQ[1:len(s.pushingQ)]
	return v, nil
}

func (s *FilebaseStorage) push(t time.Time) error {
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

func (s *FilebaseStorage) pushOne() error {
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

// IdealStorage :
type IdealStorage struct {
	fileInfos    *data.FileInfos
	hitRanker    Ranker
	contents     map[int]struct{}
	curSize      int64
	limitSize    int64
	updatedT     time.Time
	updatePeriod time.Duration
}

// NewIdealStorage :
func NewIdealStorage(updatePeriod, statDuration, shiftPeriod time.Duration, limitSize int64,
	fi *data.FileInfos, useSessionDuration, useFileSize, useTimeWeight bool) *IdealStorage {
	s := &IdealStorage{
		fileInfos:    fi,
		hitRanker:    NewHitRanker(statDuration, shiftPeriod, fi, useSessionDuration, useFileSize, useTimeWeight),
		contents:     make(map[int]struct{}),
		limitSize:    limitSize,
		updatePeriod: updatePeriod,
	}
	fmt.Printf("new nice storage updatePeriod(%v) statDuration(%v) shiftPeriod(%v) useSessionDuration(%v) useFileSize(%v) useTimeWeight(%v)\n",
		updatePeriod, statDuration, shiftPeriod, useSessionDuration, useFileSize, useTimeWeight)
	return s
}

// UpdateStart :
func (s *IdealStorage) UpdateStart(evt *data.SessionEvent) error {
	s.hitRanker.UpdateStart(evt)

	if s.updatedT.IsZero() {
		s.updatedT = evt.Time
	} else if evt.Time.Sub(s.updatedT) >= s.updatePeriod {
		s.updatedT = evt.Time
		s.update(evt.Time)
	}
	return nil
}

// UpdateEnd :
func (s *IdealStorage) UpdateEnd(evt *data.SessionEvent) {
	s.hitRanker.UpdateEnd(evt)
}

// Exists :
func (s *IdealStorage) Exists(file int) bool {
	_, ok := s.contents[file]
	return ok
}

// LimitSize :
func (s *IdealStorage) LimitSize() int64 {
	return s.limitSize
}

func (s *IdealStorage) update(t time.Time) {
	contents := make(map[int]struct{})
	list := s.hitRanker.HitList(nil)
	var empty struct{}
	var totalSize int64
	for _, v := range list {
		totalSize += v.filesize
		if s.limitSize < totalSize {
			break
		}
		contents[v.filename] = empty
	}
	s.contents = contents
}
