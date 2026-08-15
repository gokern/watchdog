package watchdog_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/gokern/watchdog"
)

// The package owns no goroutine and no ticker. goleak is what keeps that true:
// add a background driver and this fails, whatever the rest of the suite says.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestWiringPanics(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		want string
		call func()
	}{
		"zero-value watchdog registers a track": {
			want: "watchdog: Watchdog must be created with New",
			call: func() {
				var w watchdog.Watchdog

				w.Track("consumer", watchdog.MaxSilence(time.Second))
			},
		},
		"zero-value watchdog arms": {
			want: "watchdog: Watchdog must be created with New",
			call: func() {
				var w watchdog.Watchdog

				w.Arm()
			},
		},
		"empty name": {
			want: "watchdog: empty track name",
			call: func() {
				watchdog.New().Track("", watchdog.MaxSilence(time.Second))
			},
		},
		// A name reaches watchdogprom as a label value, which has to be UTF-8.
		"name that is not valid UTF-8": {
			want: `watchdog: track name "bad\xffname" is not valid UTF-8`,
			call: func() {
				watchdog.New().Track("bad\xffname", watchdog.MaxSilence(time.Second))
			},
		},
		"no predicate": {
			want: `watchdog: track "consumer" has no predicate`,
			call: func() {
				watchdog.New().Track("consumer")
			},
		},
		"concurrency without the predicate that tracks units": {
			want: `watchdog: track "consumer" sets Concurrency without NoStuckUnit`,
			call: func() {
				watchdog.New().Track("consumer",
					watchdog.MaxSilence(time.Second),
					watchdog.Concurrency(8),
				)
			},
		},
		"duplicate name": {
			want: `watchdog: duplicate track "consumer"`,
			call: func() {
				w := watchdog.New()
				w.Track("consumer", watchdog.MaxSilence(time.Second))
				w.Track("consumer", watchdog.MaxSilence(time.Second))
			},
		},
		"register after arming": {
			want: "watchdog: Track after Arm",
			call: func() {
				w := watchdog.New()
				w.Track("consumer", watchdog.MaxSilence(time.Second))
				w.Arm()
				w.Track("late", watchdog.MaxSilence(time.Second))
			},
		},
		"arm twice": {
			want: "watchdog: Arm called twice",
			call: func() {
				w := watchdog.New()
				w.Track("consumer", watchdog.MaxSilence(time.Second))
				w.Arm()
				w.Arm()
			},
		},
		"arm with no tracks": {
			want: "watchdog: Arm with no tracks",
			call: func() {
				watchdog.New().Arm()
			},
		},
		"zero silence window": {
			want: "watchdog: MaxSilence needs a positive window",
			call: func() {
				watchdog.New().Track("consumer", watchdog.MaxSilence(0))
			},
		},
		"negative silence window": {
			want: "watchdog: MaxSilence needs a positive window",
			call: func() {
				watchdog.New().Track("consumer", watchdog.MaxSilence(-time.Second))
			},
		},
		"zero stuck-unit bound": {
			want: "watchdog: NoStuckUnit needs a positive bound",
			call: func() {
				watchdog.New().Track("responder", watchdog.NoStuckUnit(0))
			},
		},
		"zero concurrency": {
			want: "watchdog: Concurrency needs a peak of 1..65536",
			call: func() {
				watchdog.New().Track("responder",
					watchdog.NoStuckUnit(time.Second),
					watchdog.Concurrency(0),
				)
			},
		},
		"concurrency above the bound": {
			want: "watchdog: Concurrency needs a peak of 1..65536",
			call: func() {
				watchdog.New().Track("responder",
					watchdog.NoStuckUnit(time.Second),
					watchdog.Concurrency(65537),
				)
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.PanicsWithValue(t, tc.want, tc.call)
		})
	}
}

func TestArmingIsWhatStartsMeasuring(t *testing.T) {
	t.Parallel()

	t.Run("not live before arming", func(t *testing.T) {
		t.Parallel()

		w := watchdog.New()
		w.Track("consumer", watchdog.MaxSilence(time.Hour))

		require.False(t, w.Live(), "an unarmed watchdog must not report live")
	})

	t.Run("snapshot names the tracks before arming", func(t *testing.T) {
		t.Parallel()

		w := watchdog.New()
		w.Track("consumer", watchdog.MaxSilence(time.Hour))
		w.Track("responder", watchdog.NoStuckUnit(time.Hour))

		snap := w.Snapshot()
		require.Len(t, snap, 2, "a forgotten Arm must still be diagnosable")

		for _, s := range snap {
			require.NotEmpty(t, s.Name)
			require.False(t, s.Fresh, "nothing is measured before Arm, so nothing is fresh")
		}
	})

	t.Run("live once armed, before any unit runs", func(t *testing.T) {
		t.Parallel()

		w := watchdog.New()
		w.Track("consumer", watchdog.MaxSilence(time.Hour))
		w.Arm()

		require.True(t, w.Live(), "arming starts each track's window")
		require.False(t, w.Snapshot()[0].Seen, "no unit has completed yet")
	})

	// Callers index the slice by position, so the order is part of the API.
	// Every other fixture in the suite happens to register its tracks in
	// alphabetical order, which leaves sorting by name indistinguishable.
	t.Run("snapshot follows registration order, not name order", func(t *testing.T) {
		t.Parallel()

		w := watchdog.New()
		w.Track("zebra", watchdog.MaxSilence(time.Hour))
		w.Track("alpha", watchdog.MaxSilence(time.Hour))
		w.Arm()

		snap := w.Snapshot()
		require.Equal(t, "zebra", snap[0].Name)
		require.Equal(t, "alpha", snap[1].Name)
	})

	// Options run before Track takes its lock, so one may register another
	// track. Hoisting the lock above them deadlocks, which no assertion can
	// catch -- only this test failing to return.
	t.Run("an option may register another track", func(t *testing.T) {
		t.Parallel()

		w := watchdog.New()
		w.Track("primary", watchdog.MaxSilence(time.Hour), func(*watchdog.Track) {
			w.Track("secondary", watchdog.MaxSilence(time.Hour))
		})
		w.Arm()

		require.Len(t, w.Snapshot(), 2)
	})

	// In a bubble, because the stale track's window is a nanosecond and only a
	// fake clock can be relied on to have passed it. On the real clock this
	// asserted on the 83ns that happen to elapse between Arm and Live on a
	// developer machine, which a platform with a coarser monotonic clock reads
	// as no time at all.
	t.Run("one stale track takes the whole process down", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New()
			healthy := w.Track("healthy", watchdog.MaxSilence(time.Hour))
			w.Track("never-runs", watchdog.MaxSilence(time.Nanosecond))
			w.Arm()

			healthy.Do(func() {})
			time.Sleep(time.Millisecond)

			require.False(t, w.Live(), "Live is an AND, not a vote")
			require.True(t, w.Snapshot()[0].Fresh)
			require.False(t, w.Snapshot()[1].Fresh)
		})
	})
}
