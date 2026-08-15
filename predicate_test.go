package watchdog_test

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gokern/watchdog"
)

// The bubble fakes the monotonic clock the package reads, so a window measured
// in minutes expires in microseconds and lands on the exact nanosecond asked
// for. Boundary cases are unreachable with a real clock.
//
// One caveat the tests below respect: a unit started on another goroutine reads
// the clock a nanosecond after the root does, so its age is asserted by which
// side of the bound it falls on, never by an exact value.

// wedge starts a unit that blocks until the returned function is called. That
// function is safe to call twice, so a test can both defer it and call it at
// the point it means to: an assertion failing inside a bubble unwinds the root
// goroutine, and a bubble whose other goroutines are still blocked panics over
// the top of the real message.
func wedge(t *testing.T, track *watchdog.Track) func() {
	t.Helper()

	blocked := make(chan struct{})
	release := sync.OnceFunc(func() { close(blocked) })

	go track.Do(func() { <-blocked })

	synctest.Wait()

	return release
}

func TestMaxSilence(t *testing.T) {
	t.Parallel()

	t.Run("silence inside the window is not failure", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("consumer", watchdog.MaxSilence(time.Minute))
			w.Arm()

			track.Do(func() {})
			time.Sleep(59 * time.Second)

			require.True(t, w.Live())
		})
	})

	t.Run("silence past the window is", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("consumer", watchdog.MaxSilence(time.Minute))
			w.Arm()

			track.Do(func() {})
			time.Sleep(61 * time.Second)

			require.False(t, w.Live())
			require.Equal(t, 61*time.Second, w.Snapshot()[0].Silence)
		})
	})

	t.Run("the boundary belongs to the fresh side", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("consumer", watchdog.MaxSilence(time.Minute))
			w.Arm()

			track.Do(func() {})
			time.Sleep(time.Minute)

			require.True(t, w.Live(), "a track is fresh at exactly its window")

			time.Sleep(time.Nanosecond)

			require.False(t, w.Live(), "and stale one nanosecond later")
		})
	})

	t.Run("a completed unit restarts the window", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("consumer", watchdog.MaxSilence(time.Minute))
			w.Arm()

			for range 5 {
				time.Sleep(50 * time.Second)
				track.Do(func() {})
				require.True(t, w.Live())
			}
		})
	})

	t.Run("a failing unit still proves the strand turns", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("poller", watchdog.MaxSilence(time.Minute))
			w.Arm()

			time.Sleep(30 * time.Second)
			// The broker is down and every attempt fails. The loop is alive,
			// and reporting a dependency outage through liveness would turn one
			// outage into a fleet-wide restart.
			for range 10 {
				track.Do(func() { _ = "attempt failed" })
				time.Sleep(3 * time.Second)
			}

			require.True(t, w.Live())
		})
	})

	t.Run("a strand that never starts is caught by its own window", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			w.Track("goroutine-never-launched", watchdog.MaxSilence(time.Minute))
			w.Arm()

			require.True(t, w.Live(), "arming grants a window for the first proof")

			time.Sleep(61 * time.Second)

			require.False(t, w.Live())
			require.False(t, w.Snapshot()[0].Seen, "it never completed a unit")
		})
	})
}

func TestNoStuckUnit(t *testing.T) {
	t.Parallel()

	t.Run("idleness never makes a track stale", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			w.Track("responder", watchdog.NoStuckUnit(time.Second))
			w.Arm()

			time.Sleep(24 * time.Hour)

			require.True(t, w.Live(), "no traffic is not a fault")
		})
	})

	t.Run("a unit wedged past its bound is", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("responder", watchdog.NoStuckUnit(9*time.Second))
			w.Arm()

			release := wedge(t, track)
			defer release()

			time.Sleep(8 * time.Second)

			require.True(t, w.Live(), "still inside its bound")
			require.Equal(t, 1, w.Snapshot()[0].InFlight)

			time.Sleep(2 * time.Second)

			require.False(t, w.Live())
			require.Greater(t, w.Snapshot()[0].Oldest, 9*time.Second)

			release()
			synctest.Wait()

			require.True(t, w.Live(), "the track recovers when the unit completes")
			require.Zero(t, w.Snapshot()[0].InFlight)
		})
	})

	t.Run("a dispatcher that dies while idle is the documented blind spot", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("responder", watchdog.NoStuckUnit(time.Second))
			w.Arm()

			track.Do(func() {}) // one request handled, then the goroutine dies
			time.Sleep(24 * time.Hour)

			require.True(t, w.Live(),
				"NoStuckUnit cannot see a strand that stopped between units")
			require.Equal(t, 24*time.Hour, w.Snapshot()[0].Silence,
				"Silence is the only field that shows it")
		})
	})
}

// The peaks exist so that a window can be sized from what a strand did rather
// than from what someone guessed at the composition root.
func TestPeaks(t *testing.T) {
	t.Parallel()

	t.Run("the largest gap outlives the smaller ones after it", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("consumer", watchdog.MaxSilence(time.Hour))
			w.Arm()

			track.Do(func() {})
			time.Sleep(30 * time.Second)
			track.Do(func() {})
			time.Sleep(5 * time.Second)
			track.Do(func() {})

			s := w.Snapshot()[0]
			require.Equal(t, 30*time.Second, s.PeakSilence, "the worst gap is what sizes a window")
			require.Zero(t, s.Silence, "while Silence is only the current one")
		})
	})

	t.Run("the wait for the first proof is not a gap between two", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("consumer", watchdog.MaxSilence(time.Hour))
			w.Arm()

			time.Sleep(20 * time.Minute) // a strand that is slow to connect
			track.Do(func() {})

			require.Zero(t, w.Snapshot()[0].PeakSilence,
				"a slow start must not stand as the peak forever")
		})
	})

	t.Run("the longest unit outlives the shorter ones after it", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("responder", watchdog.NoStuckUnit(time.Hour))
			w.Arm()

			track.Do(func() { time.Sleep(9 * time.Second) })
			track.Do(func() { time.Sleep(time.Second) })

			// A unit's age spans a sleep, and the bubble's clock lands a
			// nanosecond off across that boundary, so this asserts the value
			// rather than the exact tick.
			require.InDelta(t, float64(9*time.Second),
				float64(w.Snapshot()[0].PeakUnit), float64(time.Microsecond))
		})
	})

	t.Run("a track that only bounds its silence measures no unit", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("consumer", watchdog.MaxSilence(time.Hour))
			w.Arm()

			track.Do(func() { time.Sleep(9 * time.Second) })

			require.Zero(t, w.Snapshot()[0].PeakUnit,
				"it never reads the clock when a unit begins, so it cannot know")
		})
	})
}

// Arm is where measurement starts, and both halves of that matter: the wiring
// before it spends none of the window, and whatever ran before it leaves no
// measurement behind.
func TestArmStartsMeasuring(t *testing.T) {
	t.Parallel()

	t.Run("the window opens at Arm, not at New", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			w.Track("consumer", watchdog.MaxSilence(time.Minute))

			time.Sleep(10 * time.Minute) // a root that dials its dependencies first

			w.Arm()

			require.True(t, w.Live(), "the wiring before Arm spends none of the window")
			require.Zero(t, w.Snapshot()[0].Silence)
		})
	})

	t.Run("a unit completed before Arm leaves nothing behind", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("consumer",
				watchdog.MaxSilence(time.Hour),
				watchdog.NoStuckUnit(time.Hour),
			)

			// A component handed its track and started before the root armed.
			track.Do(func() { time.Sleep(45 * time.Minute) })
			time.Sleep(30 * time.Minute)
			track.Do(func() {})

			w.Arm()

			s := w.Snapshot()[0]
			require.False(t, s.Seen, "a proof before Arm is not a proof")
			require.Zero(t, s.PeakSilence, "nor is a gap before Arm a gap")
			require.Zero(t, s.PeakUnit, "nor is a unit before Arm a unit")

			// And the interval from Arm to the first proof stays excluded.
			time.Sleep(20 * time.Minute)
			track.Do(func() {})

			require.Zero(t, w.Snapshot()[0].PeakSilence,
				"the wait for the first proof is not a gap, whatever ran before Arm")
		})
	})

	// Arm discards measurements, not units. Clearing the slots here would read
	// as a tidy reset and would make a strand that wedged before the process
	// armed invisible for the rest of its life.
	t.Run("a unit already in flight survives Arm", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("responder", watchdog.NoStuckUnit(time.Minute))

			release := wedge(t, track)
			defer release()

			w.Arm()

			require.Equal(t, 1, w.Snapshot()[0].InFlight,
				"arming must not forget a unit that is still running")

			time.Sleep(2 * time.Minute)

			require.False(t, w.Live(),
				"a strand wedged before Arm is still wedged after it")

			release()
			synctest.Wait()
		})
	})

	// Overflows is exported as a Prometheus counter. Resetting it at Arm would
	// show up as a rate() spike during the incident it exists to explain.
	t.Run("overflows counted before Arm are not forgotten", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("responder",
				watchdog.NoStuckUnit(time.Minute),
				watchdog.Concurrency(1), // the floor: 8 slots
			)

			blocked := make(chan struct{})
			release := sync.OnceFunc(func() { close(blocked) })

			defer release()

			for range 9 { // one more than the array holds
				go track.Do(func() { <-blocked })
			}

			synctest.Wait()
			require.Equal(t, int64(1), w.Snapshot()[0].Overflows)

			w.Arm()

			require.Equal(t, int64(1), w.Snapshot()[0].Overflows,
				"a counter must not go backwards")

			release()
			synctest.Wait()
		})
	})
}

func TestBoundaries(t *testing.T) {
	t.Parallel()

	t.Run("a unit is in flight, not stuck, at exactly its bound", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("responder", watchdog.NoStuckUnit(time.Minute))
			w.Arm()

			release := wedge(t, track)
			defer release()

			// The unit reads the clock a nanosecond after the root, so the root
			// sleeps one further to land the unit's age exactly on the bound.
			time.Sleep(time.Minute + time.Nanosecond)

			require.True(t, w.Live())
			require.Equal(t, time.Minute, w.Snapshot()[0].Oldest)
			require.True(t, w.Snapshot()[0].Fresh,
				"Snapshot must put the bound where Live puts it")

			time.Sleep(time.Nanosecond)

			require.False(t, w.Live(), "and stuck one nanosecond later")
			require.False(t, w.Snapshot()[0].Fresh)

			release()
			synctest.Wait()
		})
	})

	t.Run("Snapshot puts the silence boundary where Live does", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("consumer", watchdog.MaxSilence(time.Minute))
			w.Arm()

			track.Do(func() {})
			time.Sleep(time.Minute)

			require.True(t, w.Snapshot()[0].Fresh)

			time.Sleep(time.Nanosecond)

			require.False(t, w.Snapshot()[0].Fresh)
		})
	})

	t.Run("Oldest is the oldest unit, not whichever the scan met last", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("responder", watchdog.NoStuckUnit(time.Minute))
			w.Arm()

			stuck := wedge(t, track)
			defer stuck()

			time.Sleep(90 * time.Second)

			// Traffic keeps arriving while the first unit is wedged. The newest
			// unit sits well inside the bound; the oldest is what decides.
			recent := wedge(t, track)
			defer recent()

			s := w.Snapshot()[0]
			require.Equal(t, 2, s.InFlight)
			require.Greater(t, s.Oldest, time.Minute)
			require.False(t, s.Fresh)
			require.False(t, w.Live(), "a wedge behind fresh traffic is the whole point")

			stuck()
			recent()
			synctest.Wait()
		})
	})
}

func TestPredicatesCompose(t *testing.T) {
	t.Parallel()

	t.Run("a wedged unit is caught before the silence window expires", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("consumer",
				watchdog.MaxSilence(time.Minute),
				watchdog.NoStuckUnit(20*time.Second),
			)
			w.Arm()

			release := wedge(t, track)
			defer release()

			time.Sleep(21 * time.Second)

			require.False(t, w.Live(), "the tighter bound decides, 39s before silence would")

			release()
			synctest.Wait()

			require.True(t, w.Live())
		})
	})

	t.Run("either predicate alone can fail the track", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("consumer",
				watchdog.MaxSilence(time.Minute),
				watchdog.NoStuckUnit(2*time.Minute),
			)
			w.Arm()

			track.Do(func() {})
			time.Sleep(61 * time.Second)

			require.False(t, w.Live(), "silence expired while no unit was in flight at all")
		})
	})
}
