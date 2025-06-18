VERSION=$(shell git describe --tags --dirty --always)

LDFLAGS += -extldflags '-static' -w
LDFLAGS += -X github.com/jhoblitt/fido/version.Version=$(VERSION)

.PHONY: all
all: lint build

.PHONY: build
build:
	CGO_ENABLED=0 go build -ldflags "${LDFLAGS}"

.PHONY: lint
lint:
	golangci-lint run
