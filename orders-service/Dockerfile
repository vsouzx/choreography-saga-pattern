FROM golang:1.25.8-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o orders-service ./cmd/api

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/orders-service .
COPY --from=builder /app/config.yaml .

EXPOSE 8081

CMD ["./orders-service"]
