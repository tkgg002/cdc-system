#!/bin/bash
set -e

HEADER="package api

import (
	\"encoding/json\"
	\"errors\"
	\"strconv\"
	\"strings\"
	\"time\"

	\"cdc-cms-service/internal/app/commands\"
	\"cdc-cms-service/internal/app/ports\"
	\"cdc-cms-service/internal/app/queries\"
	\"cdc-cms-service/internal/infra/messaging\"
	\"cdc-cms-service/internal/infra/persistence\"
	\"cdc-cms-service/internal/middleware\"
	\"cdc-cms-service/internal/model\"
	\"cdc-cms-service/pkgs/natsconn\"

	\"github.com/gofiber/fiber/v2\"
	\"go.uber.org/zap\"
	\"gorm.io/gorm\"
)
"

function slice() {
    file=$1
    shift
    echo "$HEADER" > "$file"
    for range in "$@"; do
        sed -n "${range}p" internal/api/registry_handler.go >> "$file"
    done
}

slice internal/api/registry_handler_read.go 65,96 338,347 436,443
slice internal/api/registry_handler_register.go 97,159
slice internal/api/registry_handler_update.go 160,274
slice internal/api/registry_handler_bulk.go 275,337
slice internal/api/registry_handler_tools.go 348,388 389,435 516,555 608,649
slice internal/api/registry_handler_transform.go 457,482 483,515
slice internal/api/registry_handler_dispatch.go 556,607

# Modify original registry_handler.go to only contain lines 1-64 and 444-456 and 650-684
echo "$HEADER" > internal/api/registry_handler_new.go
sed -n "24,64p" internal/api/registry_handler.go >> internal/api/registry_handler_new.go
sed -n "444,456p" internal/api/registry_handler.go >> internal/api/registry_handler_new.go
sed -n "650,684p" internal/api/registry_handler.go >> internal/api/registry_handler_new.go
mv internal/api/registry_handler_new.go internal/api/registry_handler.go

~/go/bin/goimports -w internal/api/registry_handler*.go

