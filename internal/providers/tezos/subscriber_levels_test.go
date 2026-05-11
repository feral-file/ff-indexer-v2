package tezos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestGetSortedLevels verifies that levels are returned in ascending order.
func TestGetSortedLevels(t *testing.T) {
	tests := []struct {
		name     string
		buffers  map[uint64]*levelBuffer
		expected []uint64
	}{
		{
			name:     "empty buffers",
			buffers:  map[uint64]*levelBuffer{},
			expected: []uint64{},
		},
		{
			name: "single level",
			buffers: map[uint64]*levelBuffer{
				100: {level: 100},
			},
			expected: []uint64{100},
		},
		{
			name: "multiple levels in order",
			buffers: map[uint64]*levelBuffer{
				100: {level: 100},
				101: {level: 101},
				102: {level: 102},
			},
			expected: []uint64{100, 101, 102},
		},
		{
			name: "multiple levels out of order",
			buffers: map[uint64]*levelBuffer{
				102: {level: 102},
				100: {level: 100},
				101: {level: 101},
			},
			expected: []uint64{100, 101, 102},
		},
		{
			name: "non-sequential levels",
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
			result := getSortedLevels(tt.buffers)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestIsLevelComplete verifies the level completion logic.
func TestIsLevelComplete(t *testing.T) {
	tests := []struct {
		name                 string
		level                uint64
		highestTransferLevel uint64
		highestBigmapLevel   uint64
		expected             bool
	}{
		{
			name:                 "level is complete when both feeds moved past",
			level:                100,
			highestTransferLevel: 101,
			highestBigmapLevel:   101,
			expected:             true,
		},
		{
			name:                 "level not complete when transfers not moved past",
			level:                100,
			highestTransferLevel: 100,
			highestBigmapLevel:   101,
			expected:             false,
		},
		{
			name:                 "level not complete when bigmaps not moved past",
			level:                100,
			highestTransferLevel: 101,
			highestBigmapLevel:   100,
			expected:             false,
		},
		{
			name:                 "level not complete when neither feed moved past",
			level:                100,
			highestTransferLevel: 100,
			highestBigmapLevel:   100,
			expected:             false,
		},
		{
			name:                 "level is complete when both feeds far ahead",
			level:                100,
			highestTransferLevel: 110,
			highestBigmapLevel:   105,
			expected:             true,
		},
		{
			name:                 "level at current highest is not complete",
			level:                105,
			highestTransferLevel: 105,
			highestBigmapLevel:   105,
			expected:             false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isLevelComplete(tt.level, tt.highestTransferLevel, tt.highestBigmapLevel)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestHasLevelTimedOut verifies the timeout logic.
func TestHasLevelTimedOut(t *testing.T) {
	now := time.Now()
	timeout := 60 * time.Second

	tests := []struct {
		name      string
		firstSeen time.Time
		timeout   time.Duration
		now       time.Time
		expected  bool
	}{
		{
			name:      "not timed out when within timeout",
			firstSeen: now.Add(-30 * time.Second),
			timeout:   timeout,
			now:       now,
			expected:  false,
		},
		{
			name:      "timed out when exceeded timeout",
			firstSeen: now.Add(-70 * time.Second),
			timeout:   timeout,
			now:       now,
			expected:  true,
		},
		{
			name:      "not timed out when at exact timeout boundary",
			firstSeen: now.Add(-60 * time.Second),
			timeout:   timeout,
			now:       now,
			expected:  false,
		},
		{
			name:      "timed out when just over timeout",
			firstSeen: now.Add(-61 * time.Second),
			timeout:   timeout,
			now:       now,
			expected:  true,
		},
		{
			name:      "not timed out for very recent events",
			firstSeen: now.Add(-1 * time.Second),
			timeout:   timeout,
			now:       now,
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasLevelTimedOut(tt.firstSeen, tt.timeout, tt.now)
			require.Equal(t, tt.expected, result)
		})
	}
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
