// Package watchdogprom reports a [watchdog.Watchdog] to Prometheus.
//
// It lives in its own module so that watchdog itself keeps no dependencies: a
// process that never scrapes anything should not carry a metrics client in its
// build graph.
//
// The bit a watchdog produces goes to the orchestrator, which acts on it by
// restarting the process. These metrics are the other half — the part that
// says which strand stopped turning, and what its windows looked like while it
// still was.
package watchdogprom

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
)

// ErrInvalidOption reports a setting this package refuses. Every option error
// wraps it, so a composition root can tell a misconfigured collector from any
// other startup failure with errors.Is.
var ErrInvalidOption = errors.New("watchdogprom: invalid option")

func invalidOption(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidOption, fmt.Sprintf(format, args...))
}

const (
	// defaultNamespace prefixes every metric name.
	defaultNamespace = "watchdog"
	// trackLabel names the label this package sets on every per-track metric.
	trackLabel = "track"
	// reservedLabelPrefix is Prometheus's own reservation for internal labels.
	reservedLabelPrefix = "__"
)

// metricNamePart is Prometheus's rule for a metric name, minus the colon that
// is reserved for recording rules.
var metricNamePart = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Option configures New.
type Option struct {
	apply func(*config) error
}

type config struct {
	namespace   string
	constLabels prometheus.Labels
}

// WithNamespace replaces the metric name prefix. The default is "watchdog", so
// a track's freshness is watchdog_track_fresh. An empty namespace drops the
// prefix entirely, which is worth doing only in a process that exports nothing
// else this could collide with.
func WithNamespace(namespace string) Option {
	return Option{apply: func(cfg *config) error {
		if namespace != "" && !metricNamePart.MatchString(namespace) {
			return invalidOption("namespace %q is not a valid metric name prefix", namespace)
		}

		cfg.namespace = namespace

		return nil
	}}
}

// WithConstLabels attaches labels to every metric this package produces.
//
// They are constant for the life of the process, so they suit a replica or
// deployment identifier and nothing that varies per track. Two names are
// refused: "track", because this package sets it per metric, and anything
// beginning with "__", which Prometheus reserves. Values are checked too, for
// the UTF-8 a label value has to be. Each of these would otherwise surface as
// a descriptor the registry rejects, and through MustRegister that is a panic
// during startup.
func WithConstLabels(labels prometheus.Labels) Option {
	return Option{apply: func(cfg *config) error {
		for name, value := range labels {
			switch {
			case name == trackLabel:
				return invalidOption("label %q is set per track and cannot be constant", name)
			case strings.HasPrefix(name, reservedLabelPrefix):
				return invalidOption("label %q uses the reserved %q prefix",
					name, reservedLabelPrefix)
			case !metricNamePart.MatchString(name):
				return invalidOption("label %q is not a valid label name", name)
			case !utf8.ValidString(value):
				return invalidOption("the value of label %q is not valid UTF-8", name)
			}
		}

		cfg.constLabels = labels

		return nil
	}}
}

// buildConfig applies opts over the defaults, refusing a bad setting rather
// than clamping it: a metric that silently reports under the wrong name is
// worse than a process that will not start.
func buildConfig(opts []Option) (config, error) {
	cfg := config{
		namespace:   defaultNamespace,
		constLabels: nil,
	}

	for _, opt := range opts {
		if opt.apply == nil {
			return config{}, invalidOption("a zero-value Option was passed to New")
		}

		err := opt.apply(&cfg)
		if err != nil {
			return config{}, err
		}
	}

	return cfg, nil
}
