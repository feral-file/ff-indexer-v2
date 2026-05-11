package tezos

import (
	"time"
)

// getSortedLevels returns sorted level numbers from the buffer map.
// Levels are sorted in ascending order to ensure deterministic processing.
func getSortedLevels(buffers map[uint64]*levelBuffer) []uint64 {
	levels := make([]uint64, 0, len(buffers))
	for level := range buffers {
		levels = append(levels, level)
	}

	// Sort in ascending order
	for i := 0; i < len(levels); i++ {
		for j := i + 1; j < len(levels); j++ {
			if levels[i] > levels[j] {
				levels[i], levels[j] = levels[j], levels[i]
			}
		}
	}

	return levels
}

// isLevelComplete checks if a level has received events from both feeds.
// A level is complete when we've seen a higher level from both feeds,
// indicating both feeds have moved past this level.
func isLevelComplete(level, highestTransferLevel, highestBigmapLevel uint64) bool {
	// Both feeds have moved past this level
	return level < highestTransferLevel && level < highestBigmapLevel
}

// hasLevelTimedOut checks if a level has exceeded the timeout duration.
// Only applies to the latest buffered level to handle feed lag gracefully.
func hasLevelTimedOut(firstSeen time.Time, timeout time.Duration, now time.Time) bool {
	return now.Sub(firstSeen) > timeout
}
