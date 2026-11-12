FROM golang:1.22 AS builder

WORKDIR /build

COPY . .

RUN go mod download
RUN go build -o unity-alerts main.go

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y ca-certificates && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/unity-alerts .

CMD ["./unity-alerts"]
