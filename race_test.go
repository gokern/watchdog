package watchdog_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gokern/watchdog"
)

// A track handed to its caller and then lost to a racing append would be a
// strand Live never reduces over, so registration must not fail open when it
// runs from more than one goroutine.
func TestConcurrentRegistration(t *testing.T) {
	t.Parallel()

	for range 100 {
		w := watchdog.New()

		var wg sync.WaitGroup

		for i := range 8 {
			wg.Go(func() {
				w.Track(fmt.Sprintf("strand-%d", i), watchdog.MaxSilence(time.Hour))
			})
		}

		wg.Wait()
		w.Arm()

		require.Len(t, w.Snapshot(), 8, "no track may be handed out and then dropped")
		require.True(t, w.Live(), "arming must have started every track's window")
	}
}

// Snapshot's lock exists so a snapshot can be taken while the watchdog is
// still being wired up. Without a reader racing the registrations, nothing
// depends on that lock being there.
func TestSnapshotDuringRegistration(t *testing.T) {
	t.Parallel()

	for range 100 {
		w := watchdog.New()

		var wg sync.WaitGroup

		for i := range 8 {
			wg.Go(func() {
				w.Track(fmt.Sprintf("strand-%d", i), watchdog.MaxSilence(time.Hour))
			})
		}

		wg.Go(func() {
			for range 50 {
				_ = w.Snapshot()
			}
		})

		wg.Wait()
		w.Arm()

		require.Len(t, w.Snapshot(), 8)
	}
}

// Live reads the track slice without the mutex, which is sound only because
// armed is stored after the last append and checked before the slice is
// touched. Nothing exercised that ordering: reversing the two in Live passes
// the whole suite under -race until a reader runs during registration.
func TestLiveDuringRegistration(t *testing.T) {
	t.Parallel()

	for range 100 {
		w := watchdog.New()

		var wg sync.WaitGroup

		for i := range 8 {
			wg.Go(func() {
				w.Track(fmt.Sprintf("strand-%d", i), watchdog.MaxSilence(time.Hour))
			})
		}

		wg.Go(func() {
			for range 50 {
				_ = w.Live()
			}
		})

		wg.Wait()
	}
}

// The probe handler is already serving while the composition root arms, so the
// flag Live reads is written while it is being read. Demoting armed to a plain
// bool passes every other test in the suite, race detector included.
func TestArmIsSafeWhileTheProbeReads(t *testing.T) {
	t.Parallel()

	for range 100 {
		w := watchdog.New()
		w.Track("consumer", watchdog.MaxSilence(time.Hour))

		var wg sync.WaitGroup

		wg.Go(func() {
			for range 50 {
				_ = w.Live()
				_ = w.Snapshot()
			}
		})

		wg.Go(w.Arm)

		wg.Wait()
	}
}

// Registering while another goroutine arms is a wiring mistake, and the panic
// reporting it is a legitimate outcome here. A data race is not: Arm takes the
// same lock the append does, and dropping it shows up nowhere else.
func TestArmIsSafeWhileTracksStillRegister(t *testing.T) {
	t.Parallel()

	for range 100 {
		w := watchdog.New()
		w.Track("first", watchdog.MaxSilence(time.Hour))

		var wg sync.WaitGroup

		for i := range 4 {
			wg.Go(func() {
				defer func() { _ = recover() }() // "Track after Arm" is allowed

				w.Track(fmt.Sprintf("late-%d", i), watchdog.MaxSilence(time.Hour))
			})
		}

		wg.Go(w.Arm)

		wg.Wait()
	}
}

// Two goroutines reaching the same free slot must not both leave with it, or a
// unit runs with nothing recording that it is in flight. A compare-and-swap is
// what prevents that, and only real parallelism with units held open can tell
// the difference between it and a load followed by a store.
func TestSlotsAreClaimedExclusively(t *testing.T) {
	t.Parallel()

	const workers = 64

	for range 200 {
		w := watchdog.New()
		track := w.Track("responder",
			watchdog.NoStuckUnit(time.Hour),
			watchdog.Concurrency(workers),
		)
		w.Arm()

		blocked := make(chan struct{})
		started := make(chan struct{}, workers)

		var wg sync.WaitGroup

		for range workers {
			wg.Go(func() {
				track.Do(func() {
					started <- struct{}{}

					<-blocked
				})
			})
		}

		// Every worker has claimed before it sends, so the assertion below runs
		// at a known point rather than after a guessed delay.
		for range workers {
			<-started
		}

		require.Equal(t, workers, w.Snapshot()[0].InFlight,
			"every unit in flight must hold a slot of its own")

		close(blocked)
		wg.Wait()
	}
}

func TestSlotsSurviveConcurrentUse(t *testing.T) {
	t.Parallel()

	w := watchdog.New()
	track := w.Track("responder",
		watchdog.NoStuckUnit(time.Hour),
		watchdog.Concurrency(64),
	)
	w.Arm()

	var wg sync.WaitGroup

	for range 16 {
		wg.Go(func() {
			for range 2000 {
				track.Do(func() {})
			}
		})
	}

	wg.Wait()

	s := w.Snapshot()[0]
	require.Zero(t, s.InFlight, "every claimed slot was released")
	require.Zero(t, s.Overflows, "16 goroutines cannot exhaust 256 slots")
	require.True(t, w.Live())
}

// hammer runs units on track from the given number of goroutines until the
// returned function is called, which also waits for them to stop. It is safe
// to call that function twice, so a test can defer it and still choose when
// the load ends.
func hammer(track *watchdog.Track, workers int) func() {
	done := make(chan struct{})

	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
					track.Do(func() {})
				}
			}
		})
	}

	return sync.OnceFunc(func() {
		close(done)
		wg.Wait()
	})
}

// keepSampling runs the load loop for a fixed floor, and past it only while
// nothing has come back stale. On a platform whose monotonic clock advances in
// milliseconds, the hammering refreshes lastProof within a tick of every
// reading, so a fixed window can legitimately contain no stale sample at all
// and the assertion that one exists would fail on a healthy package.
func keepSampling(floor, limit time.Time, stale int) bool {
	now := time.Now()

	return now.Before(floor) || (stale == 0 && now.Before(limit))
}

// Live and Snapshot run on the probe's goroutine while units complete on the
// workers'. A Status assembled from two scans of that moving state could report
// stale with every age inside its bound, or an age fractionally in the future.
func TestSnapshotStaysConsistentUnderLoad(t *testing.T) {
	t.Parallel()

	// The bound sits near the pace the load runs at, so the hammering crosses
	// it constantly. Sized generously every sample would be fresh, the
	// contradiction below could not arise, and the assertion guarding against
	// it would hold for want of anything to test.
	const bound = 50 * time.Microsecond

	w := watchdog.New()
	track := w.Track("hot",
		watchdog.MaxSilence(bound),
		watchdog.NoStuckUnit(bound),
	)
	w.Arm()

	stop := hammer(track, 8)
	defer stop()

	var samples, negative, contradictory, stale int

	floor := time.Now().Add(200 * time.Millisecond)
	limit := time.Now().Add(3 * time.Second)

	for keepSampling(floor, limit, stale) {
		for _, s := range w.Snapshot() {
			samples++

			if s.Silence < 0 || s.Oldest < 0 {
				negative++
			}

			if !s.Fresh {
				stale++
			}

			if !s.Fresh && s.Silence <= bound && s.Oldest <= bound {
				contradictory++
			}
		}
	}

	stop()

	require.Positive(t, samples, "the load loop must have produced snapshots")
	require.Positive(t, stale, "the bound must be tight enough for the load to cross it")
	require.Zero(t, negative, "an age must never be reported in the future")
	require.Zero(t, contradictory, "freshness and the ages beside it come from one scan")
}
