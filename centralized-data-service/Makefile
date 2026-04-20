.PHONY: run build test migrate infra down tidy

# Run CDC Worker
run:
	go run cmd/worker/main.go

# Build binary
build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/worker ./cmd/worker/

# Run tests
test:
	go test ./... -v -count=1

# Start infra (docker)
infra:
	docker compose up -d mysql mongodb nats postgres redis

# Run database migration
migrate:
	docker exec -i gpay-postgres psql -U user -d goopay_dw < migrations/001_init_schema.sql

# Stop all
down:
	docker compose down

# Tidy deps
tidy:
	go mod tidy
