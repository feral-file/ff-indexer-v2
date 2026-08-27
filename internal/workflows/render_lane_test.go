package workflows

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestRenderLane pins the admission rules: shared first looks, exclusive confirmations,
// nothing ever waits, and a turned-away confirmation is guaranteed its turn.
func TestRenderLane(t *testing.T) {
	t0 := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	t.Run("first looks share; a confirmation needs the lane empty", func(t *testing.T) {
		var l renderLane
		assert.True(t, l.tryEnter(false, t0))
		assert.True(t, l.tryEnter(false, t0), "first looks run side by side")
		assert.False(t, l.tryEnter(true, t0), "a confirmation cannot join in-flight first looks")
		l.leave(false)
		assert.False(t, l.tryEnter(true, t0), "one first look still in flight")
		l.leave(false)
		assert.True(t, l.tryEnter(true, t0), "empty lane admits the confirmation")
	})

	t.Run("a confirmation in flight excludes everything", func(t *testing.T) {
		var l renderLane
		assert.True(t, l.tryEnter(true, t0))
		assert.False(t, l.tryEnter(false, t0), "first looks are turned away, not parked")
		assert.False(t, l.tryEnter(true, t0), "a second confirmation is turned away")
		l.leave(true)
		assert.True(t, l.tryEnter(true, t0), "the second confirmation enters on its retry")
	})

	t.Run("a turned-away confirmation holds new first looks until it enters", func(t *testing.T) {
		var l renderLane
		assert.True(t, l.tryEnter(false, t0))
		assert.False(t, l.tryEnter(true, t0), "turned away behind a first look")
		assert.False(t, l.tryEnter(false, t0.Add(time.Second)), "new first looks are refused so the lane drains")
		l.leave(false)
		assert.True(t, l.tryEnter(true, t0.Add(laneBusyRetryDelay)), "the confirmation's retry finds the lane empty")
		l.leave(true)
		assert.True(t, l.tryEnter(false, t0.Add(laneBusyRetryDelay+time.Second)), "the hold lifts once the confirmation has run")
	})

	t.Run("the hold expires if the confirmation never returns", func(t *testing.T) {
		var l renderLane
		assert.True(t, l.tryEnter(false, t0))
		assert.False(t, l.tryEnter(true, t0))
		l.leave(false)
		assert.False(t, l.tryEnter(false, t0.Add(laneDeferredHold-time.Second)), "still held inside the window")
		assert.True(t, l.tryEnter(false, t0.Add(laneDeferredHold)), "a lost confirmation must not idle the lane forever")
	})
}
