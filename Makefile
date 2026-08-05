GO ?= go
FOUNDRY ?= forge

.PHONY: build test contracts test-contracts fmt vet lint bench run-indexer run-arbitrage run-liquidator migrate dev up down clean

build:
	$(GO) build ./...

test:
	$(GO) test ./...

contracts:
	cd contracts && $(FOUNDRY) build

test-contracts:
	cd contracts && $(FOUNDRY) test

fmt:
	gofmt -w ./cmd ./internal

vet:
	$(GO) vet ./...

bench:
	$(GO) run ./cmd/rh-cli bench

run-indexer:
	$(GO) run ./cmd/rh-indexer -config configs/robinhood.yaml

run-arbitrage:
	$(GO) run ./cmd/rh-arbitrage -config configs/robinhood.yaml

run-liquidator:
	$(GO) run ./cmd/rh-liquidator -config configs/morpho.yaml

migrate:
	docker compose -f docker-compose.dev.yml exec -T postgres psql -U rh -d rh -f /docker-entrypoint-initdb.d/0001_init.sql

dev: up
	$(GO) run ./cmd/rh-cli config-check

up:
	docker compose -f docker-compose.dev.yml up -d

down:
	docker compose -f docker-compose.dev.yml down

clean:
	rm -rf contracts/out contracts/cache
