FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o cms-service ./cmd/server/

FROM alpine:3.20
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/cms-service .
COPY --from=builder /app/config/config-local.yml ./config/config-local.yml
COPY --from=builder /app/docs ./docs
EXPOSE 8080
CMD ["./cms-service"]
