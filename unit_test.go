package watchdog_test

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gokern/watchdog"
)

func TestOnlyCompletionProves(t *testing.T) {
	t.Parallel()

	t.Run("a unit in flight is not a proof", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("consumer", watchdog.MaxSilence(time.Minute))
			w.Arm()

			track.Do(func() {})
			time.Sleep(59 * time.Second)
			require.True(t, w.Live(), "still inside the window")

			// The next unit begins 59 seconds into a 60 second window. If
			// entering Do counted as progress, this would restart the window
			// and the wedge below would go unnoticed.
			release := wedge(t, track)
			defer release()

			time.Sleep(2 * time.Second)

			require.False(t, w.Live(), "entering Do proves nothing")

			release()
			synctest.Wait()

			require.True(t, w.Live(), "returning from it does")
		})
	})

	t.Run("a panicking unit proves progress, and the panic still propagates", func(t *testing.T) {
		t.Parallel()

		w := watchdog.New()
		track := w.Track("consumer", watchdog.MaxSilence(time.Hour))
		w.Arm()

		require.PanicsWithValue(t, "handler blew up", func() {
			track.Do(func() { panic("handler blew up") })
		})

		require.True(t, w.Snapshot()[0].Seen,
			"the strand turned, whether or not the work succeeded")
		require.True(t, w.Live())
	})

	// A silence-only track keeps no slots, and Do skips the claim entirely.
	// Without that guard every unit sweeps an empty array, fails, and is
	// counted as an overflow -- which only watchdogprom's tests would notice,
	// and CI runs the two modules as separate jobs.
	t.Run("a silence-only track claims no slot", func(t *testing.T) {
		t.Parallel()

		w := watchdog.New()
		track := w.Track("consumer", watchdog.MaxSilence(time.Hour))
		w.Arm()

		for range 100 {
			track.Do(func() {})
		}

		s := w.Snapshot()[0]
		require.Zero(t, s.InFlight)
		require.Zero(t, s.Overflows,
			"a track with no slots must not report failing to claim one")
	})

	t.Run("a panicking unit releases its slot", func(t *testing.T) {
		t.Parallel()

		w := watchdog.New()
		track := w.Track("responder", watchdog.NoStuckUnit(time.Hour))
		w.Arm()

		for range 100 {
			require.Panics(t, func() {
				track.Do(func() { panic("boom") })
			})
		}

		require.Zero(t, w.Snapshot()[0].InFlight, "no slot leaked over a hundred panics")
		require.Zero(t, w.Snapshot()[0].Overflows)
	})
}

func TestSlotSizing(t *testing.T) {
	t.Parallel()

	t.Run("a power of two, never below the declared peak", func(t *testing.T) {
		t.Parallel()

		bad := 0

		for peak := 1; peak <= 65536; peak++ {
			n := watchdog.SlotsFor(peak)
			if n&(n-1) != 0 || n < peak || n < 8 {
				bad = peak

				break
			}
		}

		require.Zero(t, bad, "every peak Concurrency admits must size to a usable array")
	})

	t.Run("refuses a peak Concurrency would have rejected", func(t *testing.T) {
		t.Parallel()

		// Unguarded, this arithmetic fails quietly and in the dangerous
		// direction; slotsFor says how.
		for _, peak := range []int{0, -1, 65537, math.MaxInt} {
			require.PanicsWithValue(t,
				"watchdog: slot sizing outside the range Concurrency enforces",
				func() { watchdog.SlotsFor(peak) },
			)
		}
	})
}

func TestOverflow(t *testing.T) {
	t.Parallel()

	t.Run("units beyond capacity still run, and are counted", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("responder",
				watchdog.NoStuckUnit(time.Minute),
				watchdog.Concurrency(8), // sized for 8*4 = 32 slots
			)
			w.Arm()

			blocked := make(chan struct{})
			release := sync.OnceFunc(func() { close(blocked) })

			defer release()

			var ran atomic.Int64

			for range 100 {
				go track.Do(func() {
					ran.Add(1)
					<-blocked
				})
			}

			synctest.Wait()

			require.Equal(t, int64(100), ran.Load(), "the package never blocks the work")

			s := w.Snapshot()[0]
			require.Equal(t, 32, s.InFlight)
			require.Equal(t, int64(68), s.Overflows)

			release()
			synctest.Wait()

			require.Zero(t, w.Snapshot()[0].InFlight, "capacity comes back")
		})
	})

	t.Run("an undeclared peak still holds a real burst", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("responder", watchdog.NoStuckUnit(time.Minute))
			w.Arm()

			blocked := make(chan struct{})
			release := sync.OnceFunc(func() { close(blocked) })

			defer release()

			// A track that declares nothing is sized for defaultConcurrency,
			// which slotsFor turns into 256 slots. Lower that default and these
			// units fall into the blind spot, on the tracks least likely to have
			// had their sizing thought about at all.
			for range 256 {
				go track.Do(func() { <-blocked })
			}

			synctest.Wait()

			s := w.Snapshot()[0]
			require.Equal(t, 256, s.InFlight)
			require.Zero(t, s.Overflows)

			release()
			synctest.Wait()
		})
	})

	t.Run("declaring the peak engineers the condition away", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("responder",
				watchdog.NoStuckUnit(time.Minute),
				watchdog.Concurrency(100),
			)
			w.Arm()

			blocked := make(chan struct{})
			release := sync.OnceFunc(func() { close(blocked) })

			defer release()

			for range 100 {
				go track.Do(func() { <-blocked })
			}

			synctest.Wait()

			s := w.Snapshot()[0]
			require.Equal(t, 100, s.InFlight)
			require.Zero(t, s.Overflows)

			release()
			synctest.Wait()
		})
	})

	// The hole this leaves is deliberate and has no sound alternative: the
	// overflow path records nothing about the unit it dropped, so "the
	// untracked set has not drained" and "an untracked unit is stuck" are the
	// same observation. Overflows is the signal, and Concurrency is the remedy.
	t.Run("a unit that overflowed is invisible once it wedges", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			track := w.Track("responder",
				watchdog.NoStuckUnit(time.Second),
				watchdog.Concurrency(1), // the floor: 8 slots
			)
			w.Arm()

			holders := make(chan struct{})
			releaseHolders := sync.OnceFunc(func() { close(holders) })
			stuck := make(chan struct{})
			releaseStuck := sync.OnceFunc(func() { close(stuck) })

			defer releaseHolders()
			defer releaseStuck()

			for range 8 {
				go track.Do(func() { <-holders })
			}

			synctest.Wait()

			go track.Do(func() { <-stuck }) // no slot left for this one

			synctest.Wait()
			require.Equal(t, int64(1), w.Snapshot()[0].Overflows)

			releaseHolders()
			synctest.Wait()
			time.Sleep(2 * time.Second)

			s := w.Snapshot()[0]
			require.True(t, w.Live(),
				"the wedged unit holds no slot, so nothing reports it")
			require.Zero(t, s.InFlight)
			require.Equal(t, int64(1), s.Overflows, "this is the only trace it leaves")

			releaseStuck()
			synctest.Wait()
		})
	})
}
