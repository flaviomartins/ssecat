BINARY=ssecat
PKG=./...
LDFLAGS=-X github.com/flaviomartins/ssecat/pkg/version.Version=$(shell git describe --tags --always --dirty 2>/dev/null || echo dev) \
-X github.com/flaviomartins/ssecat/pkg/version.Commit=$(shell git rev-parse --short HEAD 2>/dev/null || echo none) \
-X github.com/flaviomartins/ssecat/pkg/version.Date=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)

.PHONY: build test fmt vet lint clean release

build:
go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/ssecat

test:
go test $(PKG)

fmt:
gofmt -w $$(find . -name '*.go' -not -path './.git/*')

vet:
go vet $(PKG)

lint: fmt vet test

clean:
rm -rf bin dist

release: clean
mkdir -p dist
GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-linux-amd64 ./cmd/ssecat
GOOS=linux GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-linux-arm64 ./cmd/ssecat
GOOS=darwin GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-darwin-amd64 ./cmd/ssecat
GOOS=darwin GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-darwin-arm64 ./cmd/ssecat
GOOS=windows GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-windows-amd64.exe ./cmd/ssecat
GOOS=windows GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-windows-arm64.exe ./cmd/ssecat
