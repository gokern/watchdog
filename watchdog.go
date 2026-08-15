// Package watchdog reduces several independent proofs of progress into one
// liveness bit.
//
// A process runs several long-lived strands of work at once. Each registers as
// a named track with its own window, and the process is live only while every
// track is fresh. A proof is the completion of a unit of work, never its start:
// a wedged unit produces no proof, which is the point.
//
// The package is passive. It owns no goroutine and no ticker; Live evaluates
// the tracks when it is asked, so it composes with a probe library without
// stacking a second staleness window on top of the tracks' own.
package watchdog

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Watchdog holds the tracks a process must keep turning. Create it with New:
// the zero value is not usable and says so.
type Watchdog struct {
	base   time.Time
	mu     sync.Mutex
	tracks []*Track
	armed  atomic.Bool
}

// New returns a disarmed Watchdog with no tracks.
func New() *Watchdog {
	return &Watchdog{base: time.Now()}
}

// Track registers a named track with its own predicates and returns it. It
// panics on an empty, duplicate or non-UTF-8 name, on a track with no
// predicate, on Concurrency without NoStuckUnit, and after Arm, all of which
// are wiring mistakes. The options panic on their own invalid arguments.
func (w *Watchdog) Track(name string, opts ...TrackOption) *Track {
	w.mustBeConstructed()

	if name == "" {
		panic("watchdog: empty track name")
	}

	// A name exists to be printed and exported, and bytes that are not valid
	// UTF-8 can be neither. Left to reach a metrics label it takes the whole
	// endpoint down rather than the one track.
	if !utf8.ValidString(name) {
		panic(fmt.Sprintf("watchdog: track name %q is not valid UTF-8", name))
	}

	// The track is built before the lock below, so a TrackOption may call back
	// into Track without deadlocking. Do not hoist the lock above this.
	t := newTrack(w, name, opts...)

	// Registration is a single-goroutine phase in every sane composition root,
	// but an unsynchronised append fails open: a track lost to a racing
	// append is still handed to its caller and still works, while Live never
	// reduces over it, so wedging it could not fail the probe.
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.armed.Load() {
		panic("watchdog: Track after Arm")
	}

	for _, existing := range w.tracks {
		if existing.name == name {
			panic(fmt.Sprintf("watchdog: duplicate track %q", name))
		}
	}

	w.tracks = append(w.tracks, t)

	return t
}

// Arm fixes the set of required tracks and starts each one's silence window,
// so a track has its own window in which to produce its first proof. Nothing
// is measured before Arm and nothing may be registered after it. Units already
// in flight keep the start they were claimed with; arming does not reset them.
//
// It panics on a second call and on a watchdog with no tracks: an empty AND is
// true, and a watchdog that answers true for nothing is auto-live wearing a
// heartbeat's clothes.
func (w *Watchdog) Arm() {
	w.mustBeConstructed()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.armed.Load() {
		panic("watchdog: Arm called twice")
	}

	if len(w.tracks) == 0 {
		panic("watchdog: Arm with no tracks")
	}

	now := w.now()
	for _, t := range w.tracks {
		t.begin(now)
	}

	w.armed.Store(true)
}

// Live reports whether every track is fresh. It is false before Arm, and is
// the value to hand a probe library as its liveness predicate.
func (w *Watchdog) Live() bool {
	if !w.armed.Load() {
		return false
	}

	now := w.now()
	for _, t := range w.tracks {
		if !t.fresh(now) {
			return false
		}
	}

	return true
}

// Snapshot returns the state of every track in the order they were registered,
// so a restart can be explained afterwards. Before Arm it reports the tracks
// registered so far with none of them fresh, which is what a forgotten Arm
// looks like: a process that never goes live, and a list naming the strands
// that were waiting to be required.
func (w *Watchdog) Snapshot() []Status {
	// The lock costs nothing on a path meant for humans, and it lets a snapshot
	// be taken while the watchdog is still being wired up.
	w.mu.Lock()
	defer w.mu.Unlock()

	now := w.now()
	armed := w.armed.Load()

	out := make([]Status, 0, len(w.tracks))

	for _, t := range w.tracks {
		s := t.status(now)
		// Nothing is measured before Arm, so nothing is fresh, whatever the
		// timestamps happen to say.
		s.Fresh = s.Fresh && armed
		out = append(out, s)
	}

	return out
}

// now returns nanoseconds since construction, read from the monotonic clock.
func (w *Watchdog) now() int64 {
	return int64(time.Since(w.base))
}

// mustBeConstructed rejects a zero-value Watchdog. Its zero base time carries
// no monotonic reading, so time.Since falls back to the wall clock, overflows,
// and saturates at the maximum Duration, returning the same constant on every
// call. Every age then reads as zero and every predicate holds forever,
// silently and permanently.
func (w *Watchdog) mustBeConstructed() {
	if w.base.IsZero() {
		panic("watchdog: Watchdog must be created with New")
	}
}

// Status is one track's state at the moment of a Snapshot.
type Status struct {
	Name string
	// Fresh reports whether the track satisfied every predicate it carries.
	Fresh bool
	// Seen reports whether the track has completed a unit since Arm.
	Seen bool
	// InFlight counts the units currently holding a slot. A unit that
	// overflowed runs without one and is counted nowhere but Overflows.
	InFlight int
	// Silence is the time since the last completed unit. On a track carrying
	// only NoStuckUnit it is not a predicate, but it is the only visibility
	// into that predicate's blind spot: a dispatcher that died while idle.
	Silence time.Duration
	// Oldest is the age of the oldest unit in flight. It is zero when the
	// track is idle, and InFlight is what tells that apart from a unit whose
	// age rounded down to nothing.
	Oldest time.Duration
	// PeakSilence is the largest gap ever observed between two completed
	// units. It is what a window should be sized against, because Silence
	// alone is a sample: a gap that opens and closes between two reads of a
	// Snapshot is invisible to everything except this.
	PeakSilence time.Duration
	// PeakUnit is the longest a unit has ever taken. It is measured only on a
	// track carrying NoStuckUnit, since a track that only bounds its silence
	// never reads the clock when a unit begins, and only over units that held a
	// slot: one that overflowed is not timed either, so this understates on
	// exactly the tracks whose sizing is already wrong. See Overflows.
	PeakUnit time.Duration
	// Overflows counts units that ran untracked because claiming a slot for
	// them failed, which does not require the array to have been full. A
	// nonzero value means the declared peak is too low; see Concurrency.
	Overflows int64
}
