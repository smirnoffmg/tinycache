BINARY := bin/tinycache
IMAGE  := tinycache:latest

.PHONY: build test lint fmt docker clean dev-cluster bench bench-compare

build:
	go build -o $(BINARY) ./cmd/tinycache

test:
	go test -race -count=1 ./...

lint:
	golangci-lint run ./...

fmt:
	golangci-lint fmt ./...

docker:
	docker build -t $(IMAGE) .

clean:
	rm -rf bin/

dev-cluster:
	docker compose up -d

bench:
	go test -bench=. -benchmem -count=3 -timeout=120s ./internal/...

bench-compare:
	docker compose -f compose.bench.yaml up -d --build
	@echo "Waiting for services to accept connections..."
	@for port in 11211 11212; do \
		for i in 1 2 3 4 5 6 7 8 9 10; do \
			nc -z localhost $$port 2>/dev/null && break; \
			sleep 0.5; \
		done; \
	done
	go test -tags bench_compare -bench='Compare_(Set|Get)' -benchmem -count=1 -timeout=300s ./bench/...
	docker compose -f compose.bench.yaml down

all: fmt lint test build
