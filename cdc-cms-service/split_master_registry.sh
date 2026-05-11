#!/bin/bash
set -e

HEADER="package api

import (
	\"encoding/json\"
	\"errors\"
	\"strings\"

	\"cdc-cms-service/internal/app/commands\"
	\"cdc-cms-service/internal/app/queries\"
	\"cdc-cms-service/internal/infra/messaging\"

	\"cdc-cms-service/internal/middleware\"

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
        sed -n "${range}p" internal/api/master_registry_handler.go >> "$file"
    done
}

slice internal/api/master_registry_handler_read.go 50,53 78,94
slice internal/api/master_registry_handler_create.go 96,129 263,352
slice internal/api/master_registry_handler_approve.go 354,428 593,601
slice internal/api/master_registry_handler_reject.go 429,485
slice internal/api/master_registry_handler_toggle.go 227,261 486,544
slice internal/api/master_registry_handler_swap.go 545,592

# Modify original
echo "package api

import (
	\"cdc-cms-service/internal/app/ports\"
	\"cdc-cms-service/internal/app/queries\"
	\"cdc-cms-service/internal/infra/persistence\"
	\"cdc-cms-service/pkgs/natsconn\"
	\"go.uber.org/zap\"
	\"gorm.io/gorm\"
	\"regexp\"
)
" > internal/api/master_registry_handler_new.go
sed -n "24,49p" internal/api/master_registry_handler.go >> internal/api/master_registry_handler_new.go
mv internal/api/master_registry_handler_new.go internal/api/master_registry_handler.go

~/go/bin/goimports -w internal/api/master_registry_handler*.go

