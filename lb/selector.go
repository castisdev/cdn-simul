package lb

import (
	"fmt"
	"log"
	"math"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/lb/vod"
	"github.com/castisdev/cdn/consistenthash"
)

// VODSelector :
type VODSelector interface {
	VODSelect(evt data.SessionEvent, lb *LB) (vod.Key, error)
	Init(cfg data.Config) error
}

// SameHashingWeight :
type SameHashingWeight struct {
	hash *consistenthash.Map
}

// VODSelect :
func (s *SameHashingWeight) VODSelect(evt data.SessionEvent, lb *LB) (vod.Key, error) {
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

// SelectAvailableFirst :
func SelectAvailableFirst(evt data.SessionEvent, lb *LB, vodKeys []string) (vod.Key, error) {
	for _, v := range vodKeys {
		k := vod.Key(v)
		if lb.VODs[k].LimitSessionCount < lb.VODs[k].CurSessionCount+1 || lb.VODs[k].LimitBps < lb.VODs[k].CurBps+evt.Bps {
			log.Printf("not available vod[%v], session(%v/%v) bps(%v/%v)",
				k, lb.VODs[k].CurSessionCount, lb.VODs[k].LimitSessionCount, lb.VODs[k].CurBps, lb.VODs[k].LimitBps)
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
func (s *SameWeightDup2) VODSelect(evt data.SessionEvent, lb *LB) (vod.Key, error) {
	vodKeys := s.hash.GetItems(evt.FileName)
	if len(vodKeys) >= 2 {
		vod0Avail := lb.VODs[vod.Key(vodKeys[0])].LimitBps - lb.VODs[vod.Key(vodKeys[0])].CurBps
		vod1Avail := lb.VODs[vod.Key(vodKeys[1])].LimitBps - lb.VODs[vod.Key(vodKeys[1])].CurBps
		if vod0Avail < vod1Avail {
			vodKeys[0], vodKeys[1] = vodKeys[1], vodKeys[0]
		}
	}
	return SelectAvailableFirst(evt, lb, vodKeys)
}
