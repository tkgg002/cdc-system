.PHONY: run build test swagger tidy migrate migrate-status

# Run CMS Service
run:
	go run cmd/server/main.go

# Build binary
build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/cms ./cmd/server/

# Run tests
test:
	go test ./... -v -count=1

# Generate Swagger docs
swagger:
	swag init -g cmd/server/main.go -o docs --parseDependency --parseInternal

# Tidy deps
tidy:
	go mod tidy

PG_CONTAINER ?= gpay-postgres-cdc
PG_USER      ?= gpay_admin
PG_DB        ?= cdc_dw

migrate:
	@for f in migrations/*.sql; do \
		echo "==> applying $$f"; \
		docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -v ON_ERROR_STOP=1 < $$f || exit 1; \
	done

migrate-status:
	@docker exec $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c "\dt cdc_cms.*"
