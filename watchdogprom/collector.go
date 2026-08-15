package watchdogprom

import (
	"errors"

	"github.com/gokern/watchdog"
	"github.com/prometheus/client_golang/prometheus"
)

// ErrNilWatchdog is returned by New when handed no watchdog to report on.
var ErrNilWatchdog = errors.New("watchdogprom: watchdog is nil")

// Collector reports a watchdog's tracks to Prometheus.
//
// It is a [prometheus.Collector] and registers nothing itself, so it goes into
// a registry the caller owns:
//
//	w := watchdog.New()
//	consumer := w.Track("invoice_consumer", watchdog.MaxSilence(45*time.Second))
//	w.Arm()
//
//	c, err := watchdogprom.New(w)
//	prometheus.MustRegister(c)
//
// The watchdog is read on scrape rather than on a ticker of its own, so the
// numbers are never staler than the scrape interval and no goroutine starts
// behind your back. The read scans every in-flight slot, which is microseconds
// for ordinary tracks but grows with the concurrency a track declared, so
// scrape on the order of tens of seconds.
//
// Two notes for the deployment. Every replica reports its own process, so
// aggregate with min by or by counting zeroes and never with max: one dead
// replica among ten is exactly what these metrics exist to show, and a max
// would hide it. And one Collector belongs to one watchdog; a registry handed
// two of these under one namespace refuses the second as a duplicate, which
// through MustRegister is a panic at startup.
type Collector struct {
	w *watchdog.Watchdog

	live      *prometheus.Desc
	fresh     *prometheus.Desc
	silence   *prometheus.Desc
	oldest    *prometheus.Desc
	inFlight  *prometheus.Desc
	overflows *prometheus.Desc
	seen      *prometheus.Desc

	peakSilence *prometheus.Desc
	peakUnit    *prometheus.Desc
}

// New builds a collector over a watchdog the caller owns. The watchdog need not
// be armed yet: an unarmed one reports every track as not fresh, which is what
// a forgotten Arm looks like and is worth seeing on a graph.
func New(w *watchdog.Watchdog, opts ...Option) (*Collector, error) {
	if w == nil {
		return nil, ErrNilWatchdog
	}

	cfg, err := buildConfig(opts)
	if err != nil {
		return nil, err
	}

	process := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(cfg.namespace, "", name),
			help, nil, cfg.constLabels)
	}

	track := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(cfg.namespace, "", name),
			help, []string{trackLabel}, cfg.constLabels)
	}

	return &Collector{
		w: w,

		live: process("live",
			"Whether every track is fresh. This is the bit the liveness probe publishes."),
		fresh: track("track_fresh",
			"Whether the track satisfied every predicate it carries."),
		silence: track("track_silence_seconds",
			"Time since the track last completed a unit of work."),
		oldest: track("track_oldest_unit_seconds",
			"Age of the oldest unit in flight, or zero when the track is idle."),
		inFlight: track("track_units_in_flight",
			"Units the track is currently running."),
		overflows: track("track_overflows_total",
			"Units that ran untracked because no slot was free."),
		seen: track("track_seen",
			"Whether the track has completed a unit since the process armed."),

		peakSilence: track("track_peak_silence_seconds",
			"Largest gap ever observed between two completed units."),
		peakUnit: track("track_peak_unit_seconds",
			"Longest a unit has ever taken, on tracks that measure units."),
	}, nil
}

// Describe implements [prometheus.Collector].
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs() {
		ch <- desc
	}
}

// Collect implements [prometheus.Collector] by reading the watchdog once.
//
// Once, and not twice. Asking Live separately read the clock a second time, so
// a track could go stale between the two calls and the scrape would publish a
// live process beside the very series showing it was not. Under load that was
// one scrape in two hundred, and always in the direction that keeps an alert
// on watchdog_live quiet.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	gauge := func(desc *prometheus.Desc, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
	}

	counter := func(desc *prometheus.Desc, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, value, labels...)
	}

	statuses := c.w.Snapshot()

	// Live is the AND over the tracks' freshness and false before arming, and
	// Snapshot already folds armed into every Fresh, so this agrees with it by
	// construction. The one state worth spelling out is a watchdog with no
	// tracks, where an empty AND would be vacuously true.
	live := len(statuses) > 0
	for _, s := range statuses {
		live = live && s.Fresh
	}

	gauge(c.live, boolValue(live))

	for _, s := range statuses {
		gauge(c.fresh, boolValue(s.Fresh), s.Name)
		gauge(c.silence, s.Silence.Seconds(), s.Name)
		gauge(c.oldest, s.Oldest.Seconds(), s.Name)
		gauge(c.inFlight, float64(s.InFlight), s.Name)
		gauge(c.seen, boolValue(s.Seen), s.Name)
		gauge(c.peakSilence, s.PeakSilence.Seconds(), s.Name)
		gauge(c.peakUnit, s.PeakUnit.Seconds(), s.Name)
		counter(c.overflows, float64(s.Overflows), s.Name)
	}
}

// descs is every descriptor this collector produces, in one place so that
// Describe cannot drift from Collect.
func (c *Collector) descs() []*prometheus.Desc {
	return []*prometheus.Desc{
		c.live, c.fresh, c.silence, c.oldest, c.inFlight, c.overflows, c.seen,
		c.peakSilence, c.peakUnit,
	}
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}

	return 0
}
