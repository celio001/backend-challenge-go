FROM golang:1.26.5-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/wallet-service ./cmd/wallet-service && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/migrate ./cmd/migrate

FROM alpine:3.23.3

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S wallet && \
    adduser -S wallet -G wallet

WORKDIR /app

COPY --from=builder /out/ /app/

USER wallet

EXPOSE 8081

CMD ["/app/wallet-service"]
