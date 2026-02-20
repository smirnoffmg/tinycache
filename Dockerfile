FROM golang:1.25 AS builder

WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /tinycache ./cmd/tinycache
RUN mkdir -p /data

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /tinycache /tinycache
COPY --from=builder --chown=nonroot:nonroot /data /data

EXPOSE 11211 11311 9090

ENTRYPOINT ["/tinycache"]
