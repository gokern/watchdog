package watchdog_test

import (
	"fmt"
	"time"

	"github.com/gokern/watchdog"
)

// Example shows the failure the package exists to remove: one strand keeps
// turning while another wedges, and the process is no longer reported live.
func Example() {
	w := watchdog.New()

	// A self-driving loop, judged by its silence.
	consumer := w.Track("invoice_consumer", watchdog.MaxSilence(100*time.Millisecond))

	// Driven from outside, so judged only by a unit stuck past its bound.
	responder := w.Track("notify_responder", watchdog.NoStuckUnit(100*time.Millisecond))

	w.Arm()

	// Both strands turn.
	consumer.Do(func() {})
	responder.Do(func() {})

	fmt.Println("both turning:", w.Live())

	// The responder wedges inside a handler. Its unit never completes -- until
	// the example releases it on the way out, so that the goroutine does not
	// outlive the test binary.
	wedged := make(chan struct{})
	defer close(wedged)

	go responder.Do(func() { <-wedged })

	time.Sleep(150 * time.Millisecond)

	// The consumer is still turning. Under one shared timestamp it would keep
	// reporting for both strands, and the process would stay green.
	consumer.Do(func() {})

	fmt.Println("consumer still turning:", w.Snapshot()[0].Fresh)
	fmt.Println("process live:", w.Live())

	for _, s := range w.Snapshot() {
		fmt.Printf("%-18s fresh=%-5v in flight=%d\n", s.Name, s.Fresh, s.InFlight)
	}

	// Output:
	// both turning: true
	// consumer still turning: true
	// process live: false
	// invoice_consumer   fresh=true  in flight=0
	// notify_responder   fresh=false in flight=1
}
