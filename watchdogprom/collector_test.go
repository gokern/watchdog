package watchdogprom_test

import (
	"strings"
	"testing"
	"time"

	"github.com/gokern/watchdog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/gokern/watchdog/watchdogprom"
)

// deterministic names only: the two duration gauges are asserted separately,
// since their values move with the clock.
var stableMetrics = []string{
	"watchdog_live",
	"watchdog_track_fresh",
	"watchdog_track_units_in_flight",
	"watchdog_track_overflows_total",
	"watchdog_track_seen",
}

func TestCollector(t *testing.T) {
	t.Parallel()

	t.Run("a turning strand reports live", func(t *testing.T) {
		t.Parallel()

		dog := watchdog.New()
		consumer := dog.Track("invoice_consumer", watchdog.MaxSilence(time.Hour))
		dog.Arm()
		consumer.Do(func() {})

		collector, err := watchdogprom.New(dog)
		require.NoError(t, err)

		const want = `
# HELP watchdog_live Whether every track is fresh. This is the bit the liveness probe publishes.
# TYPE watchdog_live gauge
watchdog_live 1
# HELP watchdog_track_fresh Whether the track satisfied every predicate it carries.
# TYPE watchdog_track_fresh gauge
watchdog_track_fresh{track="invoice_consumer"} 1
# HELP watchdog_track_overflows_total Units that ran untracked because no slot was free.
# TYPE watchdog_track_overflows_total counter
watchdog_track_overflows_total{track="invoice_consumer"} 0
# HELP watchdog_track_seen Whether the track has completed a unit since the process armed.
# TYPE watchdog_track_seen gauge
watchdog_track_seen{track="invoice_consumer"} 1
# HELP watchdog_track_units_in_flight Units the track is currently running.
# TYPE watchdog_track_units_in_flight gauge
watchdog_track_units_in_flight{track="invoice_consumer"} 0
`

		require.NoError(t,
			testutil.CollectAndCompare(collector, strings.NewReader(want), stableMetrics...))
	})

	t.Run("a stopped strand takes live down and says which", func(t *testing.T) {
		t.Parallel()

		dog := watchdog.New()
		healthy := dog.Track("healthy", watchdog.MaxSilence(time.Hour))
		dog.Track("stopped", watchdog.MaxSilence(time.Nanosecond))
		dog.Arm()
		healthy.Do(func() {})

		time.Sleep(time.Millisecond) // a million times the stopped track's window

		collector, err := watchdogprom.New(dog)
		require.NoError(t, err)

		const want = `
# HELP watchdog_live Whether every track is fresh. This is the bit the liveness probe publishes.
# TYPE watchdog_live gauge
watchdog_live 0
# HELP watchdog_track_fresh Whether the track satisfied every predicate it carries.
# TYPE watchdog_track_fresh gauge
watchdog_track_fresh{track="healthy"} 1
watchdog_track_fresh{track="stopped"} 0
`

		require.NoError(t, testutil.CollectAndCompare(collector, strings.NewReader(want),
			"watchdog_live", "watchdog_track_fresh"))
	})

	t.Run("an unarmed watchdog is visible as one", func(t *testing.T) {
		t.Parallel()

		dog := watchdog.New()
		dog.Track("consumer", watchdog.MaxSilence(time.Hour))

		gathered := gather(t, dog)

		require.Zero(t, gathered["watchdog_live"], "a forgotten Arm must not read as healthy")
		require.Zero(t, gathered["watchdog_track_fresh"],
			"and the track it was waiting on is named, not missing")
	})

	t.Run("a wedged unit shows its age", func(t *testing.T) {
		t.Parallel()

		dog := watchdog.New()
		responder := dog.Track("responder", watchdog.NoStuckUnit(time.Hour))
		dog.Arm()

		release := make(chan struct{})
		started := make(chan struct{})

		defer close(release)

		// Closed from inside the unit, so the slot is provably claimed once
		// this returns. Waiting on CollectAndCount instead would prove nothing:
		// it counts metrics rather than reading them, and the collector emits
		// one per track whether or not anything is in flight.
		go responder.Do(func() {
			close(started)
			<-release
		})

		<-started

		// The age comes from the monotonic clock, so asking only that it be
		// positive is a nanosecond assertion that a platform with a millisecond
		// clock cannot meet. Give it a gap worth measuring.
		time.Sleep(100 * time.Millisecond)

		// A second unit completes here, so the track's silence is near zero
		// while its oldest unit is 100ms old. Without that the two gauges
		// carry the same number and swapping them changes nothing.
		responder.Do(func() {})

		gathered := gather(t, dog)
		require.InDelta(t, 1, gathered["watchdog_track_units_in_flight"], 0,
			"a unit in flight must be counted, not merely named")
		require.GreaterOrEqual(t, gathered["watchdog_track_oldest_unit_seconds"], 0.050,
			"a unit in flight must report the age it has actually reached")
		require.Less(t, gathered["watchdog_track_silence_seconds"], 0.050,
			"the age of the oldest unit is not the time since the last completion")
	})

	// Silence is a sample taken at scrape time, so a gap that opens and closes
	// between two scrapes leaves no trace in it. The peaks are what survives.
	t.Run("the peaks carry what the samples miss", func(t *testing.T) {
		t.Parallel()

		dog := watchdog.New()
		responder := dog.Track("responder", watchdog.NoStuckUnit(time.Hour))
		dog.Arm()

		// The intervals are wide so that the last assertion takes a 50ms stall
		// to trip, rather than the single clock tick a coarse platform produces
		// on its own.
		responder.Do(func() {}) // the first proof; the wait before it is not a gap
		time.Sleep(100 * time.Millisecond)
		responder.Do(func() { time.Sleep(50 * time.Millisecond) })

		gathered := gather(t, dog)

		require.GreaterOrEqual(t, gathered["watchdog_track_peak_silence_seconds"], 0.100,
			"the gap between the two proofs")
		require.GreaterOrEqual(t, gathered["watchdog_track_peak_unit_seconds"], 0.050,
			"the duration of the longer unit")
		require.Less(t, gathered["watchdog_track_silence_seconds"], 0.050,
			"while the sample only knows about the moment it was taken")
	})

	// Fresh and seen answer different questions, and every other fixture here
	// pairs a fresh track with a seen one, so the two gauges could be swapped
	// without a test noticing. These two tracks disagree on both axes.
	t.Run("fresh and seen are not the same signal", func(t *testing.T) {
		t.Parallel()

		dog := watchdog.New()
		stopped := dog.Track("ran_then_stopped", watchdog.MaxSilence(time.Nanosecond))
		dog.Track("never_ran", watchdog.MaxSilence(time.Hour))
		dog.Arm()

		stopped.Do(func() {})

		time.Sleep(time.Millisecond) // a million times the stopped track's window

		const want = `
# HELP watchdog_track_fresh Whether the track satisfied every predicate it carries.
# TYPE watchdog_track_fresh gauge
watchdog_track_fresh{track="never_ran"} 1
watchdog_track_fresh{track="ran_then_stopped"} 0
# HELP watchdog_track_seen Whether the track has completed a unit since the process armed.
# TYPE watchdog_track_seen gauge
watchdog_track_seen{track="never_ran"} 0
watchdog_track_seen{track="ran_then_stopped"} 1
`

		require.NoError(t, testutil.CollectAndCompare(mustNew(t, dog), strings.NewReader(want),
			"watchdog_track_fresh", "watchdog_track_seen"))
	})

	// live is derived from the tracks, so an AND over none of them would be
	// vacuously true: a watchdog measuring nothing would report itself healthy.
	t.Run("a watchdog with no tracks is not live", func(t *testing.T) {
		t.Parallel()

		dog := watchdog.New()

		require.Equal(t, 1, testutil.CollectAndCount(mustNew(t, dog), "watchdog_live"),
			"the process gauge is published even with nothing to measure")
		require.Zero(t, gather(t, dog)["watchdog_live"])
	})

	t.Run("names and help survive promlint", func(t *testing.T) {
		t.Parallel()

		dog := watchdog.New()
		dog.Track("consumer", watchdog.MaxSilence(time.Hour))
		dog.Arm()

		problems, err := testutil.CollectAndLint(mustNew(t, dog))
		require.NoError(t, err)
		require.Empty(t, problems)
	})
}

func TestOptions(t *testing.T) {
	t.Parallel()

	t.Run("a namespace replaces the prefix", func(t *testing.T) {
		t.Parallel()

		dog := watchdog.New()
		dog.Track("consumer", watchdog.MaxSilence(time.Hour))
		dog.Arm()

		collector, err := watchdogprom.New(dog, watchdogprom.WithNamespace("svc"))
		require.NoError(t, err)

		require.Equal(t, 1, testutil.CollectAndCount(collector, "svc_live"))
		require.Zero(t, testutil.CollectAndCount(collector, "watchdog_live"))
	})

	t.Run("an empty namespace drops the prefix rather than failing", func(t *testing.T) {
		t.Parallel()

		dog := watchdog.New()
		dog.Track("consumer", watchdog.MaxSilence(time.Hour))
		dog.Arm()

		collector, err := watchdogprom.New(dog, watchdogprom.WithNamespace(""))
		require.NoError(t, err, "the empty namespace is documented, not a bad setting")

		require.Equal(t, 1, testutil.CollectAndCount(collector, "live"))
		require.Zero(t, testutil.CollectAndCount(collector, "watchdog_live"))
	})

	t.Run("constant labels reach every metric", func(t *testing.T) {
		t.Parallel()

		dog := watchdog.New()
		dog.Track("consumer", watchdog.MaxSilence(time.Hour))
		dog.Arm()

		collector, err := watchdogprom.New(dog,
			watchdogprom.WithConstLabels(prometheus.Labels{"replica": "3"}))
		require.NoError(t, err)

		const want = `
# HELP watchdog_live Whether every track is fresh. This is the bit the liveness probe publishes.
# TYPE watchdog_live gauge
watchdog_live{replica="3"} 1
`

		require.NoError(t,
			testutil.CollectAndCompare(collector, strings.NewReader(want), "watchdog_live"))
	})

	t.Run("a setting that would produce a broken descriptor is refused", func(t *testing.T) {
		t.Parallel()

		cases := map[string]watchdogprom.Option{
			"namespace that is not a metric name": watchdogprom.WithNamespace("svc-1"),
			"the per-track label as a constant": watchdogprom.WithConstLabels(
				prometheus.Labels{"track": "nope"}),
			"a reserved label": watchdogprom.WithConstLabels(
				prometheus.Labels{"__meta": "nope"}),
			"a label that is not a label name": watchdogprom.WithConstLabels(
				prometheus.Labels{"not a name": "nope"}),
			"a value that is not valid UTF-8": watchdogprom.WithConstLabels(
				prometheus.Labels{"replica": "\xff\xfe"}),
		}

		for name, opt := range cases {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				dog := watchdog.New()
				dog.Track("consumer", watchdog.MaxSilence(time.Hour))
				dog.Arm()

				_, err := watchdogprom.New(dog, opt)
				require.Error(t, err, "a bad setting must not reach the registry")
			})
		}
	})

	t.Run("no watchdog is an error, not a panic on scrape", func(t *testing.T) {
		t.Parallel()

		_, err := watchdogprom.New(nil)
		require.ErrorIs(t, err, watchdogprom.ErrNilWatchdog)
	})

	t.Run("a zero-value Option is refused rather than ignored", func(t *testing.T) {
		t.Parallel()

		dog := watchdog.New()
		dog.Track("consumer", watchdog.MaxSilence(time.Hour))
		dog.Arm()

		_, err := watchdogprom.New(dog, watchdogprom.Option{})
		require.ErrorIs(t, err, watchdogprom.ErrInvalidOption,
			"an option that configures nothing is a mistake, not a default")
	})
}

func mustNew(t *testing.T, dog *watchdog.Watchdog) *watchdogprom.Collector {
	t.Helper()

	collector, err := watchdogprom.New(dog)
	require.NoError(t, err)

	return collector
}

// gather collects once and returns the first sample of each metric by name.
func gather(t *testing.T, dog *watchdog.Watchdog) map[string]float64 {
	t.Helper()

	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(mustNew(t, dog))

	families, err := registry.Gather()
	require.NoError(t, err)

	out := make(map[string]float64, len(families))

	for _, family := range families {
		for _, metric := range family.GetMetric() {
			if gauge := metric.GetGauge(); gauge != nil {
				out[family.GetName()] = gauge.GetValue()

				break
			}
		}
	}

	return out
}
