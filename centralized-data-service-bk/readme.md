# Centralized Data Service (CDC Integration)

GooPay CDC system: Debezium (real-time MongoDB change stream) → Kafka → SinkWorker → cdc_internal shadow tables → Transmuter → public master tables.

## Architecture

```
MongoDB (Oplog) ──► Debezium ──► Kafka ──► SinkWorker ──► cdc_internal.<shadow> (JSONB)
                                                                │
                                                                │ Transmuter (plan v2 §R6)
                                                                ▼
                                                          public.<master> (typed columns)
                                                                ▲
                                                                │
                                                CMS API (admin plane) ← NATS cdc.cmd.*
                                                                ▲
                                                                │
                                                    CMS FE (React + AntD)
```

Post-Sprint 4: legacy batch-sync pipeline retired; sole CDC engine is the Debezium → cdc_internal → Transmuter chain. Plan: `agent/memory/workspaces/feature-cdc-integration/02_plan_airbyte_removal_v2_command_center.md`.

## Prerequisites

- Docker Desktop (Docker Engine 24+, Compose V2)
- 4GB+ RAM free
- Ports free: 5432, 8080, 13306, 14222, 16379, 17017, 18083, 18222, 19092
  > Ports prefix `1xxxx` de tranh conflict voi existing containers (some-mongo, some-nats, some-redis)

## Quick Start

### Step 1: Start infrastructure services

```bash
cd centralized-data-service

# Start infra only (MySQL, MongoDB, PostgreSQL, NATS, Redis)
docker compose up -d mysql mongodb nats postgres redis
```

Verify all services running:

```bash
docker compose ps
```

Expected: 5 containers running.

### Step 2: Init MongoDB ReplicaSet

MongoDB healthcheck auto-init ReplicaSet. Verify:

```bash
docker exec gpay-mongo mongosh --quiet --eval "rs.status().ok"
# Hoac tu host:
# mongosh mongodb://localhost:17017 --eval "rs.status().ok"
```

Expected: `1`

Nếu trả về lỗi, init thủ công:

```bash
docker exec gpay-mongo mongosh --quiet --eval "rs.initiate({_id:'rs0', members:[{_id:0, host:'mongodb:27017'}]})"
```

### Step 3: Verify MySQL Binlog

```bash
docker exec gpay-mysql mysql -u root -proot -e "SHOW VARIABLES LIKE 'log_bin'; SHOW VARIABLES LIKE 'binlog_format';"
# Hoac tu host:
# mysql -h localhost -P 13306 -u root -proot -e "SHOW VARIABLES LIKE 'log_bin';"
```

Expected: `log_bin = ON`, `binlog_format = ROW`

### Step 4: Verify PostgreSQL

```bash
docker exec gpay-postgres psql -U user -d goopay_dw -c "SELECT 1;"
```

### Step 5: Verify NATS JetStream

```bash
curl -s http://localhost:18222/varz | grep jetstream
```

Expected: `"jetstream": "enabled"`

### Step 6: Verify Redis

```bash
docker exec gpay-redis redis-cli ping
```

Expected: `PONG`

### Step 7: Run database migrations

```bash
# Tao CDC tables + management tables + JSONB Landing Zone
docker exec -i gpay-postgres psql -U user -d goopay_dw < migrations/001_init_schema.sql
```

> Chua co file migration? Tao tables theo `03_implementation.md` Section 2.

### Step 9: Start application services (khi da co code)

```bash
# Build va start CDC Worker + CMS
docker compose up -d --build cdc-worker cms-service
```

> Yeu cau Dockerfiles ton tai tai:
> - `deployments/docker/Dockerfile.worker`
> - `deployments/docker/Dockerfile.cms`

## All-in-one Commands

```bash
# Chi infra (khong can build code)
docker compose up -d mysql mongodb nats postgres redis

# Toan bo (can Dockerfiles ready)
docker compose up -d --build
```

## Services

| Service | Container | Port | Access |
|---------|-----------|------|--------|
| MySQL | gpay-mysql | 13306 | `mysql -h localhost -P 13306 -u root -proot goopay_db` |
| MongoDB | gpay-mongo | 17017 | `mongosh mongodb://localhost:17017` |
| NATS | gpay-nats | 14222, 18222 | nats://localhost:14222, http://localhost:18222 (monitoring) |
| PostgreSQL | gpay-postgres | 5432 | `psql -h localhost -U user -d goopay_dw` (password: `password`) |
| Redis | gpay-redis | 16379 | `redis-cli -h localhost -p 16379` |
| Airbyte | (abctl) | 18000, 18006 | http://localhost:18000 (UI), http://localhost:18006 (API) |
| CDC Worker | gpay-cdc-worker | - | Internal only |
| CMS Service | gpay-cms | 8080 | http://localhost:8080 |

## Common Commands

```bash
# Xem logs
docker compose logs -f cdc-worker
docker compose logs -f cms-service
docker compose logs -f postgres

# Restart 1 service
docker compose restart cdc-worker

# Stop tat ca
docker compose down

# Stop + xoa data (volumes)
docker compose down -v

# Xem trang thai
docker compose ps
```

## Troubleshooting

### MongoDB ReplicaSet khong init

```bash
docker exec gpay-mongo mongosh --quiet --eval "rs.initiate({_id:'rs0', members:[{_id:0, host:'host.docker.internal:17017'}]})"
```

### Port conflict

Neu port da bi chiem, sua mapping trong `docker-compose.yml`:

```yaml
ports:
  - "15432:5432"  # Doi host port
```


Airbyte chay ngoai docker-compose network. Dung `host.docker.internal` thay vi container name:
- MySQL: `host.docker.internal:13306`
- MongoDB: `host.docker.internal:17017`
- PostgreSQL: `host.docker.internal:5432`
- Redis: `host.docker.internal:16379`
- NATS: `host.docker.internal:14222`

### CDC Worker / CMS build fail

Can tao Dockerfiles truoc. Neu chua co code, chi start infra:

```bash
docker compose up -d mysql mongodb nats postgres redis
```

/////////////

Chạy thử toàn bộ

  # 1. Start infra
  cd centralized-data-service && make infra

  # 2. Run migrations
  make migrate
  cd ../cdc-auth-service && make migrate

  # 3. Start services (mỗi terminal)
  cd centralized-data-service && make run     # Worker :8082
  cd cdc-auth-service && make run             # Auth :8081
  cd cdc-cms-service && make run              # CMS API :8090
  cd cdc-cms-web && npm run dev               # FE :5173

  # 4. Login
  # Open http://localhost:5173 → admin / admin123

  Remaining (DevOps / Phase 2)

  - Integration test (CDC-M8) — cần Docker running
  - NATS permissions/ACL
  - PG user separation
  - True batch upsert
  - Dynamic Mapper full (Phase 2)
