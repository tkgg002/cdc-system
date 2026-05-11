# cdc-docker-dev

> **DEV-ONLY — KHÔNG deploy compose này lên prod.**
>
> - Source DBs (postgres, mongo, mysql, mariadb) trên prod = endpoint thật (RDS / MongoDB Atlas / on-prem) → đăng ký vào `connection_registry` qua admin/CMS, không spin container giả.
> - Core CDC infra (`centralized-data-service/`) trên prod deploy qua Helm + K8s (xem `centralized-data-service/deployments/k8s/`), không phải `docker compose`.
> - Network `cdc-bridge` chỉ phục vụ 2 compose local share DNS — prod dùng VPC + K8s NetworkPolicy / Service mesh.
> - Init scripts (`init/`) là seed sample data cho test, KHÔNG phải migration prod. Migration thật nằm ở `centralized-data-service/migrations/`.

| Service | Port | Vai trò |
|---|---|---|
| `gpay-postgres-source` | 5435 | demo source `goopay_source` (Debezium logical) |
| `gpay-postgres-shadow` | 5436 | data-lake `cdc_shadow` (raw landing `<prefix><src>`) | 
| `gpay-postgres-dest` | 5434 | demo destination `goopay_dest` (master + dw_<binding>) |
| `gpay-mongo` | 17017 | demo MongoDB source (replSet rs0) |
| `gpay-mysql` | 13306 | demo MySQL source |
| `gpay-mariadb` | 13307 | demo MariaDB source |

**KHÔNG nằm ở đây**:
- `gpay-postgres` (auth DB) — `cdc-auth-service/docker-compose.yml`.
- `gpay-postgres-cdc` (control plane) — `centralized-data-service/docker-compose.yml`.


## Bootstrap

```bash
docker network create cdc-bridge
cp .env.example .env   # tuỳ chỉnh password nếu cần
```

## Up / Down

```bash
# Up dev DBs trước (để có sẵn nguồn cho cdc-worker khi register-source).
docker compose -f cdc-system/cdc-docker-dev/docker-compose.yml up -d

# Up core CDC infra.
docker compose -f cdc-system/centralized-data-service/docker-compose.yml up -d

# Down (chỉ stop dev DBs, giữ core chạy):
docker compose -f cdc-system/cdc-docker-dev/docker-compose.yml down
```

## Lưu ý