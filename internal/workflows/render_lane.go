package workflows

import (
	"sync"
	"time"
)

// laneBusyRetryDelay is how long a probe job waits before retrying after the render
// lane turned it away. A render lasts tens of seconds, so a shorter delay only spends
// queue round-trips re-asking; a longer one leaves a confirmation idle after the lane
// has drained.
const laneBusyRetryDelay = 30 * time.Second

// laneDeferredHold is how long the lane keeps refusing new first looks after turning a
// confirmation away, so in-flight first looks drain and the confirmation gets in on its
// retry. Twice the retry delay covers the retry itself plus one polling interval; after
// that the confirmation is presumed gone (its job failed or was canceled) and first
// looks resume, or a lost confirmation would idle the lane forever.
const laneDeferredHold = 2 * laneBusyRetryDelay

// renderLane admits renders without ever blocking a worker slot: first looks share it,
// a confirmation needs it to itself, and a render that cannot enter is turned away for
// the caller to reschedule rather than parked in the slot.
//
// Reason: probe jobs share the media queue's bounded worker pool with media indexing.
// A blocking reader/writer lock (the first cut) let one confirmation render while the
// other slots' probes sat in Lock() — every slot occupied, no media-indexing job able to
// start, and a queued confirmation waiting through several serialized renders (#142 bot
// F2). Turning a render away costs one queue round-trip (laneBusyRetryDelay), during
// which the slot runs other jobs. Trade-offs: a confirmation turned away while first
// looks run must get in on its retry, so new first looks are refused for laneDeferredHold
// after that — they reschedule too, cheaply, and the hold expires on its own in case the
// confirmation never returns. Constraints: per executor, hence per worker process; the
// lane cannot see other processes' renders. Not a sync primitive: callers pair tryEnter
// with leave on success only.
type renderLane struct {
	mu         sync.Mutex
	firstLooks int  // shared renders in flight
	confirming bool // an exclusive render is in flight
	deferredAt time.Time
	deferred   bool // a confirmation was turned away and has not entered since
}

// tryEnter admits a render (exclusive for a confirmation) or reports that the lane is
// busy. now is the caller's clock so the deferred hold is testable.
func (l *renderLane) tryEnter(exclusive bool, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.confirming {
		if exclusive {
			l.deferred, l.deferredAt = true, now
		}
		return false
	}
	if exclusive {
		if l.firstLooks > 0 {
			l.deferred, l.deferredAt = true, now
			return false
		}
		l.confirming, l.deferred = true, false
		return true
	}
	if l.deferred && now.Sub(l.deferredAt) < laneDeferredHold {
		return false
	}
	l.deferred = false
	l.firstLooks++
	return true
}

// leave releases an admission granted by tryEnter.
func (l *renderLane) leave(exclusive bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if exclusive {
		l.confirming = false
		return
	}
	l.firstLooks--
}
