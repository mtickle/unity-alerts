FROM golang:1.27 AS builder

WORKDIR /build

COPY . .

RUN go mod download
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o unity-alerts main.go


FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/unity-alerts /unity-alerts

CMD ["/unity-alerts"]