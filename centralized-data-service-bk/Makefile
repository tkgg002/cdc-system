.PHONY: run build test \
	infra down tidy \
	migrate migrate-auth migrate-cdc migrate-dest \
	wipe wipe-auth wipe-cdc wipe-dest \
	bootstrap-local migrate-bootstrap migrate-bootstrap-local \
	reset-local

# ----------------------------------------------------------------------------
# Phase 01 Split E2E — multi-PG layout
#   gpay-postgres        : 5432  user=gpay_admin  db=gpay_auth      (auth only)
#   gpay-postgres-cdc    : 5433  user=gpay_admin  db=cdc_dw         (control+shadow)
#   gpay-postgres-dest   : 5434  user=gpay_admin  db=goopay_dest    (DW/master)
#   gpay-postgres-source : 5435  user=src_user    db=goopay_source  (CDC source)
#
# Migration subfolders:
#   ../cdc-auth-service/migrations/  → auth schema (cdc_auth_service)
#   migrations/cdc/                  → cdc_system + shadow scaffolding
#   migrations/dest/                 → master + dw_<binding> scaffolding
# ----------------------------------------------------------------------------

# ---- App ----
run:
	go run cmd/worker/main.go

build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/worker ./cmd/worker/

test:
	go test ./... -v -count=1

tidy:
	go mod tidy

# ---- Infra ----
infra:
	docker compose up -d postgres postgres-cdc postgres-dest postgres-source mongodb nats redis

down:
	docker compose down

# ---- Migrations: per-DB ----
migrate-auth:
	@set -e; for file in $$(find ../cdc-auth-service/migrations -maxdepth 1 -type f -name '*.sql' | sort); do \
		echo "==> [auth ] applying $$file"; \
		docker exec -i gpay-postgres psql -v ON_ERROR_STOP=1 -U gpay_admin -d gpay_auth < "$$file"; \
	done

migrate-cdc:
	@set -e; for file in $$(find migrations/cdc -maxdepth 1 -type f -name '*.sql' | sort); do \
		echo "==> [cdc  ] applying $$file"; \
		docker exec -i gpay-postgres-cdc psql -v ON_ERROR_STOP=1 -U gpay_admin -d cdc_dw < "$$file"; \
	done

migrate-dest:
	@set -e; for file in $$(find migrations/dest -maxdepth 1 -type f -name '*.sql' | sort); do \
		echo "==> [dest ] applying $$file"; \
		docker exec -i gpay-postgres-dest psql -v ON_ERROR_STOP=1 -U gpay_admin -d goopay_dest < "$$file"; \
	done

# Aggregate: apply all 3 in correct order (auth → cdc → dest)
migrate: migrate-auth migrate-cdc migrate-dest

# ---- Wipe: per-DB (DESTRUCTIVE) ----
wipe-auth:
	docker exec -i gpay-postgres      psql -v ON_ERROR_STOP=1 -U gpay_admin -d gpay_auth     < deployments/sql/auth/wipe_auth.sql

wipe-cdc:
	docker exec -i gpay-postgres-cdc  psql -v ON_ERROR_STOP=1 -U gpay_admin -d cdc_dw        < deployments/sql/cdc/wipe_cdc.sql

wipe-dest:
	docker exec -i gpay-postgres-dest psql -v ON_ERROR_STOP=1 -U gpay_admin -d goopay_dest   < deployments/sql/dest/wipe_dest.sql

wipe: wipe-auth wipe-cdc wipe-dest

# ---- Bootstrap (V2 control plane seed) ----
# Apply the local bootstrap slice ONLY (assumes wipe + migrate already done).
# Idempotent — safe to re-run.
bootstrap-local:
	docker exec -i gpay-postgres-cdc  psql -v ON_ERROR_STOP=1 -U gpay_admin -d cdc_dw      < deployments/sql/cdc/bootstrap_cdc_local.sql
	docker exec -i gpay-postgres-dest psql -v ON_ERROR_STOP=1 -U gpay_admin -d goopay_dest < deployments/sql/dest/bootstrap_dest_local.sql

# Template variant — symbolic placeholders, not used directly by E2E.
migrate-bootstrap: migrate
	docker exec -i gpay-postgres-cdc psql -v ON_ERROR_STOP=1 -U gpay_admin -d cdc_dw \
		< deployments/sql/cdc/bootstrap_cdc_template.sql

# One-shot fresh build: wipe → migrate (auth+cdc+dest) → bootstrap-local.
# Use this for E2E test reset.
reset-local: wipe migrate bootstrap-local

# Backwards-compat alias (kept so existing scripts/CI continue to work).
migrate-bootstrap-local: reset-local
