FROM golang:1.23.4 AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/wukongim ./cmd/wukongim

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /out/wukongim /usr/local/bin/wukongim

EXPOSE 5001 5100 5200 7000
ENTRYPOINT ["/usr/local/bin/wukongim", "-config", "/etc/wukongim/wukongim.conf"]
