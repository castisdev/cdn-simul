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
	InitHash(cfg data.Config) *consistenthash.Map
}

// SameHashingWeight :
type SameHashingWeight struct{}

// VODSelect :
func (s *SameHashingWeight) VODSelect(evt data.SessionEvent, lb *LB) (vod.Key, error) {
	if len(lb.VODs) != len(lb.Caches) || len(lb.VODs) == 0 || len(lb.Caches) == 0 {
		return "", fmt.Errorf("invalid cache/vod info")
	}
	vodKeys := lb.hash.GetItems(evt.FileName)
	return SelectAvailableFirst(evt, lb, vodKeys)
}

// InitHash :
func (s *SameHashingWeight) InitHash(cfg data.Config) *consistenthash.Map {
	hash := consistenthash.New(3000, nil)
	keyMap := make(map[string]int)
	for _, v := range cfg.VODs {
		hashWeight := 1
		keyMap[v.VodID] = hashWeight
	}
	hash.Add(keyMap)
	fmt.Println("same hashing weight")
	return hash
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

// DiffHashingWeight :
type DiffHashingWeight struct {
	SameHashingWeight
}

// InitHash :
func (s *DiffHashingWeight) InitHash(cfg data.Config) *consistenthash.Map {
	hash := consistenthash.New(3000, nil)
	keyMap := make(map[string]int)
	for _, v := range cfg.VODs {
		gb := int64(1024 * 1024 * 1024)
		hashWeight := int(math.Sqrt(float64(v.LimitBps/100000000)/float64(v.StorageSize/gb))*float64(v.StorageSize/gb)) / 10
		keyMap[v.VodID] = hashWeight
		fmt.Printf("%s: hash-weight(%v)\n", v.VodID, hashWeight)
	}
	hash.Add(keyMap)
	return hash
}

// SameWeightDup2 :
type SameWeightDup2 struct {
	SameHashingWeight
}

// VODSelect :
func (s *SameWeightDup2) VODSelect(evt data.SessionEvent, lb *LB) (vod.Key, error) {
	if len(lb.VODs) != len(lb.Caches) || len(lb.VODs) == 0 || len(lb.Caches) == 0 {
		return "", fmt.Errorf("invalid cache/vod info")
	}
	vodKeys := lb.hash.GetItems(evt.FileName)
	if len(vodKeys) >= 2 {
		vod0Avail := lb.VODs[vod.Key(vodKeys[0])].LimitBps - lb.VODs[vod.Key(vodKeys[0])].CurBps
		vod1Avail := lb.VODs[vod.Key(vodKeys[1])].LimitBps - lb.VODs[vod.Key(vodKeys[1])].CurBps
		if vod0Avail < vod1Avail {
			vodKeys[0], vodKeys[1] = vodKeys[1], vodKeys[0]
		}
	}
	return SelectAvailableFirst(evt, lb, vodKeys)
}
