BINARY := bin/tinycache
IMAGE  := tinycache:latest

.PHONY: build test lint fmt docker clean dev-cluster

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

all: fmt lint test build
