VERSION=$(shell git describe --tags --dirty --always)

LDFLAGS += -extldflags '-static'
LDFLAGS += -X github.com/jhoblitt/fido/version.Version=$(VERSION)

.PHONY: all
all:
	CGO_ENABLED=0 go build -ldflags "${LDFLAGS}"
	strip fido

.PHONY: lint
lint:
	golangci-lint run
