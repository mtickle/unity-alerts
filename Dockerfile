FROM golang:1.27-alpine AS builder

WORKDIR /build
RUN mkdir -p /tmp_dir && chmod 1777 /tmp_dir

COPY . .

RUN go mod download
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o unity-alerts main.go

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder --chown=0:0 /tmp_dir /tmp
COPY --from=builder /build/unity-alerts /unity-alerts

CMD ["/unity-alerts"]
