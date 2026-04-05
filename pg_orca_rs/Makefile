PG_CONFIG ?= /Users/jianghua/pg-install/bin/pg_config
export PGRX_PG_CONFIG_PATH = $(PG_CONFIG)

CARGO = cargo

.PHONY: all build install test test-unit test-integration clean

all: build

build:
	$(CARGO) build -p optimizer_core -p pg_bridge

install:
	cd pg_bridge && $(CARGO) pgrx install --release --pg-config $(PG_CONFIG)

test: test-unit test-integration

test-unit:
	$(CARGO) test -p optimizer_core

test-integration:
	$(CARGO) test -p pg_bridge --test integration -- --test-threads=1

clean:
	$(CARGO) clean
