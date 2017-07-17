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
	fileInfos   *data.FileInfos
	shiftPeriod time.Duration
	shiftT      time.Time
	contentHits []map[int]int64 // slot(shift 시간)별 content hit 관리
}

// NewHitRanker :
func NewHitRanker(statDuration, shiftPeriod time.Duration, fi *data.FileInfos) *HitRanker {
	hitSlotSize := int(statDuration.Minutes() / shiftPeriod.Minutes())
	f := &HitRanker{
		fileInfos:   fi,
		shiftPeriod: shiftPeriod,
		contentHits: make([]map[int]int64, hitSlotSize),
	}
	for i := 0; i < hitSlotSize; i++ {
		f.contentHits[i] = make(map[int]int64)
	}
	return f
}

// Update :
func (f *HitRanker) Update(evt *data.SessionEvent) {
	if f.shiftT.IsZero() {
		f.shiftT = evt.Time
	} else if evt.Time.Sub(f.shiftT) >= f.shiftPeriod {
		f.shift(evt.Time)
		f.shiftT = evt.Time
	}
	f.updateHit(evt)
}

// Deletable :
func (f *HitRanker) Deletable(contents map[int]struct{}, minDelSize int64) []int {
	var list []contentHit
	for k := range contents {
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

func (f *HitRanker) updateHit(evt *data.SessionEvent) {
	curIdx := len(f.contentHits) - 1
	// bps는 100Kbps보다 크다고 가정
	f.contentHits[curIdx][evt.IntFileName] += int64(evt.Bps / 100000)
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

// Storage :
type Storage struct {
	fileInfos   *data.FileInfos
	hitRanker   *HitRanker
	contents    map[int]struct{}
	curSize     int64
	limitSize   int64
	pushedT     time.Time
	pushPeriod  time.Duration
	delayPeriod time.Duration
	pushingQ    []int
}

// NewStorage :
func NewStorage(statDuration, shiftPeriod, pushPeriod time.Duration, limitSize int64, fi *data.FileInfos, initContents []string) *Storage {
	s := &Storage{
		fileInfos:  fi,
		hitRanker:  NewHitRanker(statDuration, shiftPeriod, fi),
		contents:   make(map[int]struct{}),
		limitSize:  limitSize,
		pushPeriod: pushPeriod,
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
	return s
}

// Update :
func (s *Storage) Update(evt *data.SessionEvent) error {
	s.hitRanker.Update(evt)

	if s.pushedT.IsZero() {
		s.pushedT = evt.Time
	} else if evt.Time.Sub(s.pushedT) >= s.pushPeriod {
		s.pushedT = evt.Time
		if err := s.push(); err != nil {
			return fmt.Errorf("failed to push, %v", err)
		}
	}

	return nil
}

// Exists :
func (s *Storage) Exists(file int) bool {
	_, ok := s.contents[file]
	return ok
}

func (s *Storage) pushStart(v int) {
	s.pushingQ = append(s.pushingQ, v)
}

func (s *Storage) completedPush() (int, error) {
	if len(s.pushingQ) < 2 {
		return 0, fmt.Errorf("not exists completed file")
	}
	var v int
	v, s.pushingQ = s.pushingQ[0], s.pushingQ[1:len(s.pushingQ)]
	return v, nil
}

func (s *Storage) push() error {
	compl, err := s.completedPush()
	if err == nil {
		var empty struct{}
		s.contents[compl] = empty
		s.curSize += s.fileInfos.Info(compl).Size
		fmt.Printf("added %s\n", s.fileInfos.Info(compl).File)
	}

	add, rank, err := s.hitRanker.Addable(s.contents, s.limitSize, s.pushingQ)
	if err == ErrNotExistsAddable {
		return nil
	} else if err != nil {
		return err
	}

	delSize := (s.curSize + s.fileInfos.Info(add).Size) - s.limitSize
	if delSize > 0 {
		del := s.hitRanker.Deletable(s.contents, delSize)
		for _, v := range del {
			delete(s.contents, v)
			s.curSize -= s.fileInfos.Info(v).Size
			fmt.Printf("deleted %s\n", s.fileInfos.Info(v).File)
		}
	}

	s.pushStart(add)
	fmt.Printf("add start %s, rank[%d]\n", s.fileInfos.Info(add).File, rank)

	return nil
}
