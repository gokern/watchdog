package watchdogprom_test

import (
	"fmt"
	"sort"
	"time"

	"github.com/gokern/watchdog"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/gokern/watchdog/watchdogprom"
)

// Example wires a watchdog to a registry the caller owns. The bit still goes to
// the liveness probe; these metrics are what explains a restart afterwards.
func Example() {
	dog := watchdog.New()
	consumer := dog.Track("invoice_consumer", watchdog.MaxSilence(45*time.Second))
	responder := dog.Track("notify_responder", watchdog.NoStuckUnit(9*time.Second))

	dog.Arm()

	collector, err := watchdogprom.New(dog)
	if err != nil {
		panic(err)
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)

	consumer.Do(func() {})
	responder.Do(func() {})

	families, err := registry.Gather()
	if err != nil {
		panic(err)
	}

	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, family.GetName())
	}

	sort.Strings(names)

	for _, name := range names {
		fmt.Println(name)
	}

	// Output:
	// watchdog_live
	// watchdog_track_fresh
	// watchdog_track_oldest_unit_seconds
	// watchdog_track_overflows_total
	// watchdog_track_peak_silence_seconds
	// watchdog_track_peak_unit_seconds
	// watchdog_track_seen
	// watchdog_track_silence_seconds
	// watchdog_track_units_in_flight
}
