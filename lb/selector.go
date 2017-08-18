package lb

import (
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/lb/vod"
	"github.com/castisdev/cdn/consistenthash"
)

// ErrFileNotFound :
var ErrFileNotFound = errors.New("file not found")

// VODSelector :
type VODSelector interface {
	VODSelect(evt *data.SessionEvent, lb LoadBalancer) (vod.Key, error)
	Init(cfg data.Config) error
	EndSession(evt *data.SessionEvent)
}

// SameHashingWeight :
type SameHashingWeight struct {
	hash *consistenthash.Map
}

// VODSelect :
func (s *SameHashingWeight) VODSelect(evt *data.SessionEvent, lb LoadBalancer) (vod.Key, error) {
	vodKeys := s.hash.GetItems(evt.FileName)
	return SelectAvailableFirst(evt, lb, vodKeys)
}

// Init :
func (s *SameHashingWeight) Init(cfg data.Config) error {
	hash := consistenthash.New(3000, nil)
	keyMap := make(map[string]int)
	for _, v := range cfg.VODs {
		hashWeight := 1
		keyMap[v.VodID] = hashWeight
	}
	hash.Add(keyMap)
	fmt.Println("same hashing weight")
	s.hash = hash
	return nil
}

// EndSession :
func (s *SameHashingWeight) EndSession(evt *data.SessionEvent) {
}

// SelectAvailableFirst :
func SelectAvailableFirst(evt *data.SessionEvent, lb LoadBalancer, vodKeys []string) (vod.Key, error) {
	for _, v := range vodKeys {
		k := vod.Key(v)
		vod := lb.GetVODs()[k]
		if vod.LimitSessionCount < vod.CurSessionCount+1 || vod.LimitBps < vod.CurBps+evt.Bps {
			log.Printf("not available vod[%v], session(%v/%v) bps(%v/%v)",
				k, vod.CurSessionCount, vod.LimitSessionCount, vod.CurBps, vod.LimitBps)
			continue
		}
		return k, nil
	}
	return "", fmt.Errorf("failed to select vod")
}

// WeightStorageBps :
type WeightStorageBps struct {
	SameHashingWeight
}

// Init :
func (s *WeightStorageBps) Init(cfg data.Config) error {
	hash := consistenthash.New(3000, nil)
	keyMap := make(map[string]int)
	for _, v := range cfg.VODs {
		gb := int64(1024 * 1024 * 1024)
		hashWeight := int(math.Sqrt(float64(v.LimitBps/100000000)/float64(v.StorageSize/gb))*float64(v.StorageSize/gb)) / 10
		keyMap[v.VodID] = hashWeight
		fmt.Printf("%s: hash-weight(%v)\n", v.VodID, hashWeight)
	}
	hash.Add(keyMap)
	s.hash = hash
	return nil
}

// WeightStorage :
type WeightStorage struct {
	SameHashingWeight
}

// Init :
func (s *WeightStorage) Init(cfg data.Config) error {
	hash := consistenthash.New(100, nil)
	keyMap := make(map[string]int)
	for _, v := range cfg.VODs {
		gb := int64(1024 * 1024 * 1024)
		hashWeight := int(v.StorageSize / (100 * gb))
		keyMap[v.VodID] = hashWeight
		fmt.Printf("%s: hash-weight(%v)\n", v.VodID, hashWeight)
	}
	hash.Add(keyMap)
	s.hash = hash
	return nil
}

// SameWeightDup2 :
type SameWeightDup2 struct {
	SameHashingWeight
}

// VODSelect :
func (s *SameWeightDup2) VODSelect(evt *data.SessionEvent, lb LoadBalancer) (vod.Key, error) {
	vodKeys := s.hash.GetItems(evt.FileName)
	if len(vodKeys) >= 2 {
		vod0 := lb.GetVODs()[vod.Key(vodKeys[0])]
		vod1 := lb.GetVODs()[vod.Key(vodKeys[1])]
		vod0Avail := vod0.LimitBps - vod0.CurBps
		vod1Avail := vod1.LimitBps - vod1.CurBps
		if vod0Avail < vod1Avail {
			vodKeys[0], vodKeys[1] = vodKeys[1], vodKeys[0]
		}
	}
	return SelectAvailableFirst(evt, lb, vodKeys)
}

// HighLowGroup : 1시간동안 인기 순위 100위 내 파일들은 고성능 서버 그룹으로
type HighLowGroup struct {
	WeightStorage
	highHash        *consistenthash.Map
	contentHits     map[int]int64
	updatedHotListT time.Time
	updateHotPeriod time.Duration
	hotList         []HitInfo
	hotThreshold    int
}

// NewHighLowGroup :
func NewHighLowGroup(updateHotPeriod time.Duration, hotRankThreshold int) VODSelector {
	return &HighLowGroup{
		contentHits:     make(map[int]int64),
		updateHotPeriod: updateHotPeriod,
		hotThreshold:    hotRankThreshold,
	}
}

// Init :
func (s *HighLowGroup) Init(cfg data.Config) error {
	fmt.Printf("high-low: update-period:%v hot-rank:%v\n", s.updateHotPeriod, s.hotThreshold)
	lowHash := consistenthash.New(1000, nil)
	highHash := consistenthash.New(1000, nil)
	lowKeyMap := make(map[string]int)
	highKeyMap := make(map[string]int)
	for _, v := range cfg.VODs {
		high := true
		if v.LimitBps < 5000000000 {
			high = false
		}
		GB := int64(1024 * 1024 * 1024)
		Gbps := int64(1000 * 1000 * 1000)

		hashWeight := int(v.StorageSize / (100 * GB))
		lowKeyMap[v.VodID] = hashWeight
		fmt.Printf("%s: hash-weight(%v)\n", v.VodID, hashWeight)

		if high {
			highWeight := int(v.LimitBps / (1 * Gbps))
			highKeyMap[v.VodID] = highWeight
			fmt.Printf("%s: (high) hash-weight(%v)\n", v.VodID, highWeight)
		}
	}
	highHash.Add(highKeyMap)
	lowHash.Add(lowKeyMap)
	s.highHash = highHash
	s.hash = lowHash
	return nil
}

// VODSelect :
func (s *HighLowGroup) VODSelect(evt *data.SessionEvent, lb LoadBalancer) (vod.Key, error) {
	if s.updatedHotListT.IsZero() {
		s.updatedHotListT = evt.Time
	} else if evt.Time.Sub(s.updatedHotListT) >= s.updateHotPeriod {
		s.updateHotList()
		s.updatedHotListT = evt.Time
	}
	// file bitrate는 100k보다 크다고 가정
	hitWeight := int64(evt.Duration.Seconds()) * int64(evt.Bps/100000)
	if v, ok := s.contentHits[evt.IntFileName]; ok {
		s.contentHits[evt.IntFileName] = v + hitWeight
	} else {
		s.contentHits[evt.IntFileName] = hitWeight
	}

	if s.isHot(evt.IntFileName) {
		vodKeys := s.highHash.GetItems(evt.FileName)
		k, err := SelectAvailableFirst(evt, lb, vodKeys)
		if err != nil {
			vodKeys = s.hash.GetItems(evt.FileName)
			return SelectAvailableFirst(evt, lb, vodKeys)
		}
		return k, err
	}
	vodKeys := s.hash.GetItems(evt.FileName)
	return SelectAvailableFirst(evt, lb, vodKeys)
}

// EndSession :
func (s *HighLowGroup) EndSession(evt *data.SessionEvent) {
}

type contentHitSorter []HitInfo

func (chs contentHitSorter) Len() int {
	return len(chs)
}
func (chs contentHitSorter) Swap(i, j int) {
	chs[i], chs[j] = chs[j], chs[i]
}
func (chs contentHitSorter) Less(i, j int) bool {
	return chs[i].hit > chs[j].hit
}

func (s *HighLowGroup) updateHotList() {
	var list []HitInfo
	for k, v := range s.contentHits {
		list = append(list, HitInfo{filename: k, hit: v})
	}
	sort.Sort(contentHitSorter(list))
	s.hotList = list

	for k := range s.contentHits {
		delete(s.contentHits, k)
	}
	for i, v := range s.hotList {
		fmt.Printf("hitlist[%4d] : %s\n", i, v)
		if i >= 9999 {
			break
		}
	}
}

func (s *HighLowGroup) isHot(file int) bool {
	for i, v := range s.hotList {
		if v.filename == file {
			return true
		}
		if i >= s.hotThreshold {
			break
		}
	}
	return false
}

// FileBase :
type FileBase struct {
	vodID   string
	storage Storage
}

// NewFileBase :
func NewFileBase(s Storage) VODSelector {
	return &FileBase{storage: s}
}

// Init :
func (s *FileBase) Init(cfg data.Config) error {
	fmt.Printf("filebase: storage:%v\n", s.storage.LimitSize())
	for _, v := range cfg.VODs {
		s.vodID = v.VodID
		break
	}
	return nil
}

// VODSelect :
func (s *FileBase) VODSelect(evt *data.SessionEvent, lb LoadBalancer) (vod.Key, error) {
	if err := s.storage.UpdateStart(evt); err != nil {
		return "", err
	}
	if s.storage.Exists(evt.IntFileName) {
		return vod.Key(s.vodID), nil
	}
	return "", ErrFileNotFound
}

// EndSession :
func (s *FileBase) EndSession(evt *data.SessionEvent) {
	s.storage.UpdateEnd(evt)
}
