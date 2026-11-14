
# This module is verified against the versions its go.mod declares, not against
# whatever a go.work above it resolves.
export GOWORK=off

default: verify

build:
	go build .

lint:
	go fmt ./...
	go fix ./...
	golangci-lint run ./...
	gosec ./...
	go vet ./...

test:
	go test -race ./...

verify: lint test build
