.PHONY: run build test swagger tidy migrate

run:
	go run cmd/server/main.go

build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/auth ./cmd/server/

test:
	go test ./... -v -count=1

swagger:
	swag init -g cmd/server/main.go -o docs --parseDependency --parseInternal

migrate:
	docker exec -i gpay-postgres psql -U user -d goopay_dw < migrations/001_auth_users.sql

tidy:
	go mod tidy
