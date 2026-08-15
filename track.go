package watchdog

import (
	"fmt"
	"math/bits"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// cacheLine is the stride between in-flight slots. Packing them costs
	// roughly half the throughput to false sharing.
	cacheLine = 128
	// defaultConcurrency is the peak a track is sized for when it declares none.
	defaultConcurrency = 64
	// maxConcurrency bounds the declared peak, and with it the memory one track
	// can reserve: 32 MiB at the top, which Live sweeps on every probe.
	maxConcurrency = 1 << 16
	// slotHeadroom multiplies the declared peak. See slotsFor for why.
	slotHeadroom = 4
	// minSlots is the floor. Without it slotsFor(1) would hand back four slots,
	// too few to absorb any burst at all.
	minSlots = 8
	// probeShift turns a timestamp into a probe index by bucketing it into
	// 2^probeShift nanoseconds, close to one tick of the monotonic clock.
	probeShift = 5
)

// slot holds the start of one unit in flight, or zero when free. The padding
// makes the stride a whole cache line. The array's base is only pointer
// aligned, so a slot can begin anywhere within a line, but slots 128 bytes
// apart never land in the same one.
type slot struct {
	start atomic.Int64
	_     [cacheLine - unsafe.Sizeof(atomic.Int64{})]byte
}

// Both arrays are empty when a slot is exactly one cache line wide and fail to
// compile when it is not. Checking both subtractions catches a slot that grew
// as well as one that shrank, so either breaks the build instead of quietly
// losing the separation the padding exists for.
var (
	_ [cacheLine - unsafe.Sizeof(slot{})]byte
	_ [unsafe.Sizeof(slot{}) - cacheLine]byte
)

// Track is one strand of work, proving its own progress under its own
// predicates. It is a concrete type on purpose: reaching it through an
// interface costs an allocation per unit, because the closure handed to Do can
// no longer be proven not to escape.
type Track struct {
	w    *Watchdog
	name string

	maxSilence  time.Duration
	noStuckUnit time.Duration

	// concurrency is the declared peak; zero means Concurrency was not given,
	// the same convention the predicates above use.
	concurrency int

	lastProof atomic.Int64
	seen      atomic.Bool
	overflows atomic.Int64

	// peakSilence and peakUnit are high-water marks kept for sizing, not for
	// the predicates. See Status for how they are meant to be read.
	peakSilence atomic.Int64
	peakUnit    atomic.Int64

	// slots is nil unless the track carries NoStuckUnit; a track that only
	// bounds its silence needs no per-unit bookkeeping at all.
	slots []slot
}

// TrackOption configures a track at registration.
type TrackOption func(*Track)

// newTrack builds a track from its options and sizes its bookkeeping. It
// panics on an option combination that cannot mean anything, which is a wiring
// mistake like the ones Track itself rejects.
func newTrack(w *Watchdog, name string, opts ...TrackOption) *Track {
	t := &Track{w: w, name: name}

	for _, opt := range opts {
		opt(t)
	}

	if t.maxSilence == 0 && t.noStuckUnit == 0 {
		panic(fmt.Sprintf("watchdog: track %q has no predicate", name))
	}

	if t.concurrency > 0 && t.noStuckUnit == 0 {
		panic(fmt.Sprintf("watchdog: track %q sets Concurrency without NoStuckUnit", name))
	}

	if t.noStuckUnit > 0 {
		peak := t.concurrency
		if peak == 0 {
			peak = defaultConcurrency
		}

		t.slots = make([]slot, slotsFor(peak))
	}

	return t
}

// MaxSilence reports the track fresh while no more than window has passed
// since it last completed a unit. Silence is failure, so this suits a loop
// that drives itself and returns control on a bounded interval.
//
// The window is the longest legitimate interval between two proofs: the wait
// before the next run, plus the worst-case execution, plus backoff and
// retries, plus scheduler, GC and load jitter. It is not a handler's budget.
func MaxSilence(window time.Duration) TrackOption {
	return func(t *Track) {
		if window <= 0 {
			panic("watchdog: MaxSilence needs a positive window")
		}

		t.maxSilence = window
	}
}

// NoStuckUnit reports the track fresh unless a unit has been in flight longer
// than bound. Idleness never makes the track stale, so this suits a strand
// driven from outside that cannot produce a proof while waiting.
//
// It is the weaker predicate: it catches a wedged handler and does not catch a
// dispatch goroutine that died while idle. Prefer MaxSilence wherever the
// strand's shape permits it.
func NoStuckUnit(bound time.Duration) TrackOption {
	return func(t *Track) {
		if bound <= 0 {
			panic("watchdog: NoStuckUnit needs a positive bound")
		}

		t.noStuckUnit = bound
	}
}

// Concurrency declares the largest number of units the track expects to have
// in flight at once and sizes its bookkeeping for it. It is meaningful only
// alongside NoStuckUnit, which is the predicate that tracks units, and panics
// without it.
//
// Declaring too low costs more than the units above the peak. Claiming walks
// the array once and can come back empty while slots are free, so a track run
// close to its capacity loses units it had room for; Status.Overflows counts
// them and a wedge among them is invisible. Declared at its true peak, a track
// sits at about a quarter of its array and loses none.
//
// By Little's law that peak is the arrival rate times the mean unit duration,
// so 100k units per second at 2.5ms each is 250 in flight. Not a rare regime.
func Concurrency(peak int) TrackOption {
	return func(t *Track) {
		if peak <= 0 || peak > maxConcurrency {
			panic(fmt.Sprintf("watchdog: Concurrency needs a peak of 1..%d", maxConcurrency))
		}

		t.concurrency = peak
	}
}

// Do runs fn as one unit of work and proves the track's progress when fn
// returns, including when fn panics: liveness asks whether the strand is
// turning, not whether the work succeeded. Entering Do proves nothing.
//
// Wrap the work, not the wait for it. Whatever sits inside fn is inside the
// unit, so a blocking wait enclosed here has to fit within NoStuckUnit's bound.
func (t *Track) Do(fn func()) {
	slotIndex := -1
	if t.slots != nil {
		slotIndex = t.claim(t.w.now())
	}

	defer t.complete(slotIndex)

	fn()
}

// slotsFor sizes a track's slot array from its declared peak. Claiming probes
// linearly, so cost climbs sharply once the array is nearly full: around four
// times the flat-case cost when a single slot is left, and an order of
// magnitude when none is. The headroom keeps ordinary operation clear of that.
// The result is a power of two, so claim can mask instead of divide.
func slotsFor(peak int) int {
	// Concurrency rejects these first; refusing them here too keeps the two
	// welded. Left to itself this arithmetic degrades in the dangerous
	// direction: a large enough peak overflows the multiplication into a
	// negative, max then picks the floor, and a track that asked for billions
	// would silently get the smallest array there is.
	if peak <= 0 || peak > maxConcurrency {
		panic("watchdog: slot sizing outside the range Concurrency enforces")
	}

	wanted := max(peak*slotHeadroom, minSlots)

	return 1 << bits.Len(uint(wanted-1))
}

// claim takes a slot for a unit starting at start, or returns -1 when one
// sweep of the array finds none free. The sweep starts from the timestamp the
// caller already holds, which costs nothing to obtain.
//
// A failed sweep does not prove the array was full. It is not atomic, so units
// freeing and retaking slots as the scan passes them can hide every free slot
// from it: the losses start around half occupancy and climb steeply above it.
// The headroom slotsFor leaves is what keeps a track clear of that regime; see
// Concurrency.
//
// Overflow drops the newest unit, the least dangerous one to drop at that
// instant, because a unit that just started cannot yet exceed the bound. It is
// not safe afterwards: nothing remembers a dropped unit. See Concurrency.
func (t *Track) claim(start int64) int {
	if start == 0 {
		// Zero marks a free slot, so a unit whose start reads as zero, which
		// can happen within the first clock tick after New, borrows the next
		// value. Units sharing a start are otherwise fine: the slot is what is
		// claimed, not the value.
		start = 1
	}

	// The slot count is a power of two, so mask is all ones and masking is the
	// modulo. Masking before the conversion keeps the probe index provably
	// within the array, whatever the timestamp was.
	mask := len(t.slots) - 1
	probe := int(start >> probeShift & int64(mask))

	for i := range len(t.slots) {
		j := (probe + i) & mask
		if t.slots[j].start.CompareAndSwap(0, start) {
			return j
		}
	}

	t.overflows.Add(1)

	return -1
}

// complete releases the unit's slot, if it had one, and records the proof.
//
// Two units completing at once can commit their timestamps out of order, so
// lastProof can move backwards by as much as a scheduling delay. That
// overstates silence, which is the safe direction, and fixing it would put a
// compare-and-swap loop on the hottest store in the package.
func (t *Track) complete(slotIndex int) {
	now := t.w.now()

	if slotIndex >= 0 {
		// The slot still holds this unit's start, so its duration costs a load
		// rather than a second reading of the clock.
		if start := t.slots[slotIndex].start.Swap(0); start != 0 {
			storeMax(&t.peakUnit, now-start)
		}
	}

	previous := t.lastProof.Load()
	t.lastProof.Store(now)

	if t.seen.Load() {
		// The interval from arming to the first proof is not an interval
		// between two proofs, and counting it would leave a slow start as the
		// peak forever.
		storeMax(&t.peakSilence, now-previous)

		return
	}

	t.seen.Store(true)
}

// begin opens the track's window at now and discards what was measured before
// it. Arm is where measurement starts, so a proof produced by a component that
// was already running is not a proof, and the gap around it is not a gap.
//
// seen is reset alongside lastProof, or a unit completed before Arm would leave
// it set and let the wait for the first proof count as a gap between two.
//
// overflows survives: a unit that ran untracked is a fault in the sizing rather
// than a measurement of the window, so it is worth the same either side of Arm,
// and it leaves this package as a counter, which should not go backwards.
func (t *Track) begin(now int64) {
	t.lastProof.Store(now)
	t.seen.Store(false)
	t.peakSilence.Store(0)
	t.peakUnit.Store(0)
}

// storeMax raises target to value, and leaves it alone when it is already
// higher. The load is the common case: once a track has run for a while a new
// maximum is rare, so the compare-and-swap almost never runs.
func storeMax(target *atomic.Int64, value int64) {
	for {
		peak := target.Load()
		if value <= peak || target.CompareAndSwap(peak, value) {
			return
		}
	}
}

// fresh reports whether the track satisfies every predicate it carries.
func (t *Track) fresh(now int64) bool {
	if t.maxSilence > 0 && now-t.lastProof.Load() > int64(t.maxSilence) {
		return false
	}

	if t.noStuckUnit > 0 {
		bound := int64(t.noStuckUnit)

		for i := range t.slots {
			start := t.slots[i].start.Load()
			if start != 0 && now-start > bound {
				return false
			}
		}
	}

	return true
}

// status collects the track's state in a single scan, so that the freshness it
// reports cannot contradict the ages reported beside it.
func (t *Track) status(now int64) Status {
	// InFlight and Oldest are filled in by the scan below.
	s := Status{
		Name:        t.name,
		Fresh:       true,
		Seen:        t.seen.Load(),
		InFlight:    0,
		Silence:     age(now - t.lastProof.Load()),
		Oldest:      0,
		PeakSilence: time.Duration(t.peakSilence.Load()),
		PeakUnit:    time.Duration(t.peakUnit.Load()),
		Overflows:   t.overflows.Load(),
	}

	var oldest int64

	for i := range t.slots {
		start := t.slots[i].start.Load()
		if start == 0 {
			continue
		}

		s.InFlight++

		if oldest == 0 || start < oldest {
			oldest = start
		}
	}

	if oldest != 0 {
		s.Oldest = age(now - oldest)
	}

	if t.maxSilence > 0 && s.Silence > t.maxSilence {
		s.Fresh = false
	}

	if t.noStuckUnit > 0 && s.Oldest > t.noStuckUnit {
		s.Fresh = false
	}

	return s
}

// age clamps a measured age at zero. The clock is read once per Live or
// Snapshot and the timestamps are loaded afterwards, so a unit completing in
// between leaves an age fractionally in the future.
func age(d int64) time.Duration {
	if d < 0 {
		return 0
	}

	return time.Duration(d)
}
