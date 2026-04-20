.PHONY: run build test swagger tidy

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
