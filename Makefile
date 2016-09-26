SOURCEDIR="."
SOURCES := $(shell find $(SOURCEDIR) -name '*.go')

BINARY=s3_sse
BINARY_RELEASE=bin/${BINARY}_${VERSION}

VERSION=$(shell cat VERSION)

.DEFAULT_GOAL: $(BINARY)

$(BINARY): bin_dir $(SOURCES)
	go build -o bin/${BINARY}

releases: release_linux_amd64

release_linux_amd64: bin_dir
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ${BINARY_RELEASE}_linux_amd64

.PHONY: bin_dir
bin_dir:
	mkdir -p bin

.PHONY: clean
clean:
	rm -f ${BINARY} ${BINARY}_*
