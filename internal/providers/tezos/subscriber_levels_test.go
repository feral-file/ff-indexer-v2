package tezos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSortedLevels(t *testing.T) {
	tests := []struct {
		name     string
		buffers  map[uint64]*levelBuffer
		expected []uint64
	}{
		{
			name:     "empty",
			buffers:  map[uint64]*levelBuffer{},
			expected: []uint64{},
		},
		{
			name:     "single",
			buffers:  map[uint64]*levelBuffer{100: {level: 100}},
			expected: []uint64{100},
		},
		{
			name: "ascending order",
			buffers: map[uint64]*levelBuffer{
				102: {level: 102},
				100: {level: 100},
				101: {level: 101},
			},
			expected: []uint64{100, 101, 102},
		},
		{
			name: "non-sequential",
			buffers: map[uint64]*levelBuffer{
				105: {level: 105},
				100: {level: 100},
				110: {level: 110},
				103: {level: 103},
			},
			expected: []uint64{100, 103, 105, 110},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, sortedLevels(tt.buffers))
		})
	}
}

func TestIsLevelComplete(t *testing.T) {
	tests := []struct {
		name                 string
		level                uint64
		highestTransferLevel uint64
		highestBigmapLevel   uint64
		expected             bool
	}{
		{
			name:                 "complete: both feeds moved past",
			level:                100,
			highestTransferLevel: 101,
			highestBigmapLevel:   101,
			expected:             true,
		},
		{
			name:                 "incomplete: transfers at level",
			level:                100,
			highestTransferLevel: 100,
			highestBigmapLevel:   101,
			expected:             false,
		},
		{
			name:                 "incomplete: bigmaps at level",
			level:                100,
			highestTransferLevel: 101,
			highestBigmapLevel:   100,
			expected:             false,
		},
		{
			name:                 "incomplete: both at level",
			level:                100,
			highestTransferLevel: 100,
			highestBigmapLevel:   100,
			expected:             false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, isLevelComplete(tt.level, tt.highestTransferLevel, tt.highestBigmapLevel))
		})
	}
}

func TestIncompleteLevelTimedOut(t *testing.T) {
	now := time.Now()
	timeout := 60 * time.Second

	require.False(t, incompleteLevelTimedOut(now, timeout, now))
	require.False(t, incompleteLevelTimedOut(now.Add(-30*time.Second), timeout, now))
	require.True(t, incompleteLevelTimedOut(now.Add(-61*time.Second), timeout, now))

	boundary := now.Add(-60 * time.Second)
	require.True(t, incompleteLevelTimedOut(boundary, timeout, now), "inclusive deadline matches nextIncompleteTimeout After(0) wake")
}

func TestNextIncompleteTimeout(t *testing.T) {
	now := time.Now()
	timeout := 60 * time.Second

	t.Run("no buffers", func(t *testing.T) {
		_, ok := nextIncompleteTimeout([]uint64{}, map[uint64]*levelBuffer{}, 100, 100, timeout, now)
		require.False(t, ok)
	})

	t.Run("timeout disabled", func(t *testing.T) {
		sorted := []uint64{100}
		buffers := map[uint64]*levelBuffer{100: {level: 100, firstSeen: now}}
		_, ok := nextIncompleteTimeout(sorted, buffers, 100, 100, 0, now)
		require.False(t, ok)
	})

	t.Run("all complete", func(t *testing.T) {
		sorted := []uint64{100}
		buffers := map[uint64]*levelBuffer{100: {level: 100, firstSeen: now}}
		_, ok := nextIncompleteTimeout(sorted, buffers, 101, 101, timeout, now)
		require.False(t, ok)
	})

	t.Run("incomplete with remaining time", func(t *testing.T) {
		sorted := []uint64{100}
		buffers := map[uint64]*levelBuffer{
			100: {level: 100, firstSeen: now.Add(-30 * time.Second)},
		}
		delay, ok := nextIncompleteTimeout(sorted, buffers, 100, 100, timeout, now)
		require.True(t, ok)
		require.Equal(t, 30*time.Second, delay)
	})

	t.Run("incomplete past deadline", func(t *testing.T) {
		sorted := []uint64{100}
		buffers := map[uint64]*levelBuffer{
			100: {level: 100, firstSeen: now.Add(-70 * time.Second)},
		}
		delay, ok := nextIncompleteTimeout(sorted, buffers, 100, 100, timeout, now)
		require.True(t, ok)
		require.Equal(t, time.Duration(0), delay)
	})

	t.Run("skips complete to find incomplete", func(t *testing.T) {
		sorted := []uint64{100, 101, 102}
		buffers := map[uint64]*levelBuffer{
			100: {level: 100, firstSeen: now.Add(-80 * time.Second)},
			101: {level: 101, firstSeen: now.Add(-70 * time.Second)},
			102: {level: 102, firstSeen: now.Add(-20 * time.Second)},
		}
		// 100 and 101 are complete (feeds at 102), 102 is incomplete
		delay, ok := nextIncompleteTimeout(sorted, buffers, 102, 102, timeout, now)
		require.True(t, ok)
		require.Equal(t, 40*time.Second, delay)
	})

	t.Run("uses earliest incomplete level's firstSeen", func(t *testing.T) {
		sorted := []uint64{100, 101}
		buffers := map[uint64]*levelBuffer{
			100: {level: 100, firstSeen: now.Add(-50 * time.Second)},
			101: {level: 101, firstSeen: now.Add(-20 * time.Second)},
		}
		// Both incomplete, should use level 100's deadline (earliest)
		delay, ok := nextIncompleteTimeout(sorted, buffers, 100, 100, timeout, now)
		require.True(t, ok)
		require.Equal(t, 10*time.Second, delay)
	})
}

// TestLevelBuffer_Structure verifies the levelBuffer structure.
func TestLevelBuffer_Structure(t *testing.T) {
	now := time.Now()

	buf := &levelBuffer{
		level:     100,
		transfers: []TzKTTokenTransfer{{Level: 100}},
		bigmaps:   []TzKTBigMapUpdate{{Level: 100}},
		firstSeen: now,
	}

	require.Equal(t, uint64(100), buf.level)
	require.Len(t, buf.transfers, 1)
	require.Len(t, buf.bigmaps, 1)
	require.Equal(t, now, buf.firstSeen)
}

// TestPruneEmittedLevels verifies the pruning logic for emittedLevels map.
func TestPruneEmittedLevels(t *testing.T) {
	// Create a mock subscriber with clock
	sub := &tzSubscriber{}

	tests := []struct {
		name                 string
		emittedLevels        map[uint64]bool
		highestTransferLevel uint64
		highestBigmapLevel   uint64
		maxBufferedLevels    int
		expectedRemaining    map[uint64]bool
	}{
		{
			name: "no pruning when minTrackedLevel below window",
			emittedLevels: map[uint64]bool{
				10: true,
				20: true,
				30: true,
			},
			highestTransferLevel: 30,
			highestBigmapLevel:   30,
			maxBufferedLevels:    20, // window = 40
			expectedRemaining: map[uint64]bool{
				10: true,
				20: true,
				30: true,
			},
		},
		{
			name: "prunes old levels beyond window",
			emittedLevels: map[uint64]bool{
				100: true,
				120: true,
				140: true,
				160: true,
			},
			highestTransferLevel: 160,
			highestBigmapLevel:   160,
			maxBufferedLevels:    20, // window = 40, threshold = 120
			expectedRemaining: map[uint64]bool{
				120: true,
				140: true,
				160: true,
			},
		},
		{
			name: "uses maximum of both feeds",
			emittedLevels: map[uint64]bool{
				100: true,
				120: true,
				140: true,
			},
			highestTransferLevel: 160, // higher, so this is used
			highestBigmapLevel:   140,
			maxBufferedLevels:    20, // window = 40, threshold = 120
			expectedRemaining: map[uint64]bool{
				120: true,
				140: true,
			},
		},
		{
			name: "handles uint64 underflow at low levels",
			emittedLevels: map[uint64]bool{
				5:  true,
				10: true,
				15: true,
			},
			highestTransferLevel: 20,
			highestBigmapLevel:   20,
			maxBufferedLevels:    20, // window = 40, but 20 < 40 so threshold = 0
			expectedRemaining: map[uint64]bool{
				5:  true,
				10: true,
				15: true,
			},
		},
		{
			name:                 "handles empty map",
			emittedLevels:        map[uint64]bool{},
			highestTransferLevel: 100,
			highestBigmapLevel:   100,
			maxBufferedLevels:    20,
			expectedRemaining:    map[uint64]bool{},
		},
		{
			name: "prunes at exact boundary",
			emittedLevels: map[uint64]bool{
				119: true,
				120: true,
				121: true,
			},
			highestTransferLevel: 160,
			highestBigmapLevel:   160,
			maxBufferedLevels:    20, // window = 40, threshold = 120
			expectedRemaining: map[uint64]bool{
				120: true,
				121: true,
			},
		},
		{
			name: "prunes based on max feed when one feed is stalled",
			emittedLevels: map[uint64]bool{
				50:  true,
				100: true,
				150: true,
				200: true,
			},
			highestTransferLevel: 0,   // Stalled feed (never advanced)
			highestBigmapLevel:   200, // Active feed
			maxBufferedLevels:    20,  // window = 40, uses max (200), threshold = 160
			expectedRemaining: map[uint64]bool{
				200: true,
			},
		},
		{
			name: "prunes based on max when feeds highly asymmetric",
			emittedLevels: map[uint64]bool{
				80:  true,
				100: true,
				120: true,
				180: true,
			},
			highestTransferLevel: 180, // High level
			highestBigmapLevel:   90,  // Lagging feed
			maxBufferedLevels:    20,  // window = 40, uses max (180), threshold = 140
			expectedRemaining: map[uint64]bool{
				180: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy to avoid modifying test input
			emittedLevels := make(map[uint64]bool)
			for k, v := range tt.emittedLevels {
				emittedLevels[k] = v
			}

			sub.pruneEmittedLevels(emittedLevels, tt.highestTransferLevel, tt.highestBigmapLevel, tt.maxBufferedLevels)

			require.Equal(t, tt.expectedRemaining, emittedLevels)
		})
	}
}
