package tezos

import "time"

// sortedLevels returns level numbers from the buffer map in ascending order.
func sortedLevels(buffers map[uint64]*levelBuffer) []uint64 {
	levels := make([]uint64, 0, len(buffers))
	for level := range buffers {
		levels = append(levels, level)
	}
	// Ascending order
	for i := 0; i < len(levels); i++ {
		for j := i + 1; j < len(levels); j++ {
			if levels[i] > levels[j] {
				levels[i], levels[j] = levels[j], levels[i]
			}
		}
	}
	return levels
}

// isLevelComplete returns true if both feeds have moved past this level.
func isLevelComplete(level, highestTransferLevel, highestBigmapLevel uint64) bool {
	return level < highestTransferLevel && level < highestBigmapLevel
}

// incompleteLevelTimedOut returns true when now has reached or passed firstSeen+levelTimeout.
// Matches nextIncompleteTimeout's deadline (inclusive boundary) so timer wake and emit agree.
func incompleteLevelTimedOut(firstSeen time.Time, levelTimeout time.Duration, now time.Time) bool {
	if levelTimeout <= 0 {
		return false
	}
	return !now.Before(firstSeen.Add(levelTimeout))
}

// nextIncompleteTimeout returns the delay until the earliest incomplete level times out,
// based on its firstSeen timestamp. Returns (0, false) if no timeout is needed.
//
// This is the key difference from runner: runner uses inactivity timeout (resets on new events),
// while Tezos uses per-level age timeout (level 100's timer starts when first level-100 data arrives,
// not reset by level 101+ data).
func nextIncompleteTimeout(
	sortedLevels []uint64,
	buffers map[uint64]*levelBuffer,
	highestTransferLevel, highestBigmapLevel uint64,
	levelTimeout time.Duration,
	now time.Time,
) (time.Duration, bool) {
	if levelTimeout <= 0 {
		return 0, false
	}
	// Find first incomplete level
	for _, level := range sortedLevels {
		buf := buffers[level]
		if buf == nil {
			continue
		}
		if isLevelComplete(level, highestTransferLevel, highestBigmapLevel) {
			continue
		}
		// Found incomplete - compute delay until its deadline
		deadline := buf.firstSeen.Add(levelTimeout)
		delay := deadline.Sub(now)
		if delay < 0 {
			delay = 0
		}
		return delay, true
	}
	return 0, false
}
