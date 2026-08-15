# watchdog — local development tooling.
#
# Two modules live here: the root, whose only dependencies are its test ones,
# and watchdogprom, which carries client_golang. Every Go tool stops at a
# module boundary, so each target below runs twice rather than relying on
# ./... to descend into the nested one. It does not.

.DEFAULT_GOAL := help

# Shuffled on purpose. Every test here calls t.Parallel and several assert on
# real elapsed time, so a fixed order is one arrangement of that rather than a
# statement about any other.
GO_TEST ?= go test -race -shuffle=on

.PHONY: help
help: ## List the targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: test
test: ## Run both modules' tests with the race detector
	$(GO_TEST) ./...
	cd watchdogprom && $(GO_TEST) ./...

.PHONY: test-cover
test-cover: ## Run the tests and report total coverage for each module
	$(GO_TEST) -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1
	cd watchdogprom && $(GO_TEST) -covermode=atomic -coverprofile=coverage.out ./... \
		&& go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint: ## Run golangci-lint over both modules
	golangci-lint run ./...
	cd watchdogprom && golangci-lint run ./...

.PHONY: fmt
fmt: ## Apply the formatters configured in .golangci.yaml
	golangci-lint fmt ./...
	cd watchdogprom && golangci-lint fmt ./...

.PHONY: tidy
tidy: ## Check both go.mod files are tidy
	go mod tidy && git diff --exit-code -- go.mod go.sum
	cd watchdogprom && go mod tidy && git diff --exit-code -- go.mod go.sum

.PHONY: vuln
vuln: ## Scan both modules for known vulnerabilities
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...
	cd watchdogprom && go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# The combination the replace hides. Every check above builds watchdogprom
# against this working tree; an adopter builds it against the version its
# require names, and nothing else here ever compiles that pair. Until the root
# is tagged and the require points at a real version, this target is meant to
# fail — that failure is the whole reason it exists.
.PHONY: release-check
release-check: ## Build watchdogprom against the published root its go.mod requires
	@ver=$$(cd watchdogprom && go list -m -f '{{.Version}}' github.com/gokern/watchdog); \
	test -n "$$ver" || { echo "watchdogprom has no require for github.com/gokern/watchdog"; exit 1; }; \
	echo "building watchdogprom against the published github.com/gokern/watchdog $$ver"; \
	tmp=$$(mktemp -d) && trap 'rm -rf "$$tmp"' EXIT || exit 1; \
	cp -R watchdogprom/. "$$tmp" && cd "$$tmp" || exit 1; \
	go mod edit -dropreplace=github.com/gokern/watchdog; \
	export GOFLAGS=-mod=mod; \
	go build ./... && go vet ./... && go test -race ./...

.PHONY: check
check: lint test vuln ## Everything CI runs before merge
