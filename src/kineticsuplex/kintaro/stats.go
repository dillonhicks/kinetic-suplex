package kintaro

import "time"

const STATS_INTERVAL = 5000 * time.Millisecond


type CacheStats struct {
	statsDelta time.Duration
	lastStats time.Time
	nextStats time.Time
	lastSeqNum uint64
	currentRps float64
}


func newStats() CacheStats {
	now := time.Now()

	return CacheStats{
		statsDelta: STATS_INTERVAL,
		lastStats: now,
		nextStats: now.Add(STATS_INTERVAL),
		lastSeqNum: 0,
		currentRps: 0,
	}
}

func (c *CacheStats) CurrentRps() float64 {
	return c.currentRps
}


func (c *CacheStats) Update(sequenceNumber uint64) {
	now := time.Now()

	if (now.After(c.nextStats)) {
		deltaT := float64(now.UnixNano()- c.lastStats.UnixNano()) / float64(time.Second)
		deltaX := float64(sequenceNumber - c.lastSeqNum)
		c.currentRps = deltaX / deltaT
		c.lastSeqNum = sequenceNumber
		c.lastStats = now
		c.nextStats = now.Add(c.statsDelta)
	}
}