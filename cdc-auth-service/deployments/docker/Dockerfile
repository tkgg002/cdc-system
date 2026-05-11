FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o auth-service ./cmd/server/

FROM alpine:3.20
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/auth-service .
COPY --from=builder /app/config ./config
EXPOSE 8081
# Runtime prod: set cfgPath=./config/config-production.yml + AUTH_DB_*/AUTH_JWT_SECRET via env.
CMD ["./auth-service"]
