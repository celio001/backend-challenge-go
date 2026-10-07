FROM golang:1.26.5-alpine AS builder

WORKDIR /api

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o backend-challenge main.go

FROM alpine:3.23.3

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S backend-challenge-user && \
    adduser -S backend-challenge-user -G backend-challenge-user

WORKDIR /app

COPY --from=builder /api/backend-challenge /app/backend-challenge

USER backend-challenge-user

EXPOSE 8081

CMD ["./backend-challenge", "api"]