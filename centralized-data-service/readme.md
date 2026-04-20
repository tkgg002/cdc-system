# Centralized Data Service (CDC Integration)

GooPay CDC Hybrid system: Airbyte (batch) + Debezium (real-time) → PostgreSQL.

## Architecture

```
MySQL (Binlog) ──┐
                  ├──→ Airbyte (batch sync) ──→ PostgreSQL (Data Warehouse)
MongoDB (Oplog) ─┘                                    │
                                                       ├── CDC Worker (Go) ← NATS
                                                       ├── CMS Service (approve schema changes)
                                                       └── Redis (cache)
```

## Prerequisites

- Docker Desktop (Docker Engine 24+, Compose V2)
- 8GB+ RAM free (Airbyte alone needs ~4GB)
- Ports free: 5432, 18000, 18006, 8080, 13306, 14222, 16379, 17017, 18222
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

### Step 8: Install Airbyte (cai rieng, khong trong docker-compose)

Airbyte la multi-container platform, khong co single Docker image. Cai bang `abctl`:

```bash
# Install abctl (macOS)
brew install airbytehq/tap/abctl

# Deploy Airbyte local on port 18000
abctl local install --port 18000
```

Sau khi cai xong:
- **Airbyte UI**: http://localhost:18000
- **Airbyte API**: http://localhost:18006
- **Credentials (User/Pass)**: Chạy lệnh `abctl local credentials` để lấy thông tin đăng nhập mặc định.

> Airbyte can ~4GB RAM. Neu may yeu, skip buoc nay va config sau.

#### Configure Airbyte connections

1. Mo http://localhost:18000
2. Tao **Source - MongoDB**:
   - Host: `host.docker.internal`
   - Port: `17017`
   - Replica Set: `rs0`
3. Tao **Source - MySQL**:
   - Host: `host.docker.internal`
   - Port: `13306`
   - User: `root`, Password: `root`
   - Database: `goopay_db`
4. Tao **Destination - PostgreSQL**:
   - Host: `host.docker.internal`
   - Port: `5432`
   - User: `user`, Password: `password`
   - Database: `goopay_dw`
5. Tao **Connections** (Source → Destination):
   - Sync mode: Incremental + Dedup
   - Schedule: 15 min (critical tables), 1 hr (non-critical)

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

### Airbyte khong connect duoc toi MySQL/MongoDB/PostgreSQL

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