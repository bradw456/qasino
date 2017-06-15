VERSION = $(shell head -1 debian/changelog | awk '{print $2}' | grep -o -e '\([0-9\.]\+\)' | tr -d '()')
UNIQUE_TAG ?= $(date +%s)
REPO ?=


GOPATH := $(shell pwd)
BIN := $(GOPATH)/bin

PATH := /usr/local/go/bin:$(PATH)

GO = /usr/local/go/bin/go

#ENV_VARS = GOPATH=$(GOPATH) CGO_LDFLAGS="-lstdc++ -lrt -Wl,-Bstatic "`pkg-config  --variable=libdir libzmq`"/libzmq.a -lpgm -lsodium -Wl,-Bdynamic"
ENV_VARS = GOPATH=$(GOPATH) CGO_LDFLAGS="-lstdc++ -lrt -lm"

all: install.deps bin/qasino-server bin/qasino-client

version:
	@echo Version is $(VERSION)

QASINO_SERVER_SRC := \
	src/qasino/server/*.go \
	src/qasino/table/*.go \
	src/qasino/util/*.go

test: $(QASINO_SERVER_SRC) src/main/qasino-server.go
	$(ENV_VARS) $(GO) test qasino/server -test.v $(TESTARGS)

bin/qasino-server: $(QASINO_SERVER_SRC) src/main/qasino-server.go
	$(ENV_VARS) $(GO) build -x -v -o $@ src/main/qasino-server.go

bin/qasino-client: src/main/qasino-client.go
	$(ENV_VARS) $(GO) build -x -v -o $@ src/main/qasino-client.go

install.deps: Godeps
	PATH=$(PATH) GOPATH=$(GOPATH) ./gpm
	touch $@


DESTDIR := install/
bindir := opt/qasino/bin
etcdir := opt/qasino/etc

install: bin/qasino-server bin/qasino-client
	install -m 755 -D bin/qasino-server $(DESTDIR)/$(bindir)/qasino-server
	install -m 644 -D etc/config.yaml.sample $(DESTDIR)/$(etcdir)/config.yaml.sample
	for file in `cd etc ; find files -type f`; do \
		install -m 644 -D etc/$${file} $(DESTDIR)/$(etcdir)/$${file} ; \
	done

clean:
	rm -f install.deps bin/qasino-server

python-docker-client:
	docker build -t "${REPO}qasino-client:${VERSION}-${UNIQUE_TAG}" -f Dockerfile.python-client .
	docker push "${REPO}qasino-client:${VERSION}-${UNIQUE_TAG}"
	echo "Client container image created."

python-docker-server:
	docker build -t "${REPO}qasino-server:${VERSION}-${UNIQUE_TAG}" -f Dockerfile.python-server.
	docker push "${REPO}qasino-server:${VERSION}-${UNIQUE_TAG}"
	echo "Server container image created"

# Use a container to build the application (e.g. golang:1.7).
# Mounts the current dir to save results.

build-with-docker:
	@echo "Building build docker image..."
	docker build --rm -f Dockerfile.build $(DOCKER_BUILD_ARGS) -t qasino_build .
	@echo "Running make via docker..."
	docker run --rm -it -v $(CURDIR):/go/src/qasino $(DOCKER_ARGS) --name qasino_build qasino_build $(ARGS)

# Build a baseimage using phusion/baseimage so we get access to the
# container friendly system things (e.g. runit, logging, my_init etc).
# Defaults to using the sample config, running my_init and logging in the container.

build-docker-baseimage: bin/qasino-server
	@echo "Building run docker image..."
	docker build --rm -f Dockerfile $(DOCKER_BUILD_ARGS) -t qasino/baseimage:$(VERSION) .

# For convenience use this target to run qasino-server from the baseimage.
# Runs it directly without my_init.  

run-with-docker: build-docker-baseimage
	@echo "Running qasino-server in docker..."
	[ -e etc/config.yaml ] || cp etc/config.yaml.sample etc/config.yaml
	docker run --rm -it -p 8080:80 -p 1443:443 $(DOCKER_ARGS) \
			-v $(CURDIR)/etc/config.yaml:/opt/qasino/etc/config.yaml \
			--entrypoint=/opt/qasino/bin/qasino-server --name qasino-server_run \
		qasino/baseimage:$(VERSION) $(ARGS)

# To run beyond dev you probably want something more like:
# 
#   docker run -e GIN_MODE=release -P \
#              -v /etc/qasino/config.yaml:/opt/qasino/etc/config.yaml \
#              -v /var/log/qasino:/opt/qasino/log \
#            qasino/baseimage:1.0


.PHONY: clean test
