# Report: Fix 502 Bad Gateway on Connector Creation

**Date**: 2026-05-08
**Issue**: `POST /api/v1/system/connectors` returned `502 Bad Gateway` with `Timeout exceeded`.

## Root Cause Analysis
1.  **Network Isolation**: The payload used `mongodb://localhost:17017`. Since Kafka Connect runs inside a Docker container, `localhost` refers to the container itself, not the host where MongoDB is mapped to 17017.
2.  **Replica Set Configuration**: The MongoDB Replica Set (`rs0`) was configured with member `host.docker.internal:17017`. This caused Kafka Connect to attempt connecting to the host, which is less efficient and prone to resolution issues so internal Docker networking is preferred.
3.  **Short Timeout**: The CMS Service had a hardcoded 10s timeout for Kafka Connect calls. This was insufficient when the connection was hanging due to network issues.

## Actions Taken
1.  **MongoDB Reconfiguration**:
    *   Updated Replica Set member host from `host.docker.internal:17017` to `gpay-mongo:27017`.
    *   Command: `docker exec gpay-mongo mongosh --eval 'cfg = rs.conf(); cfg.members[0].host = "gpay-mongo:27017"; rs.reconfig(cfg, {force: true});'`
2.  **CMS Service Update**:
    *   Modified `internal/infra/http/kafka_connect.go` to increase `httpClient` timeout from `10s` to `30s`.
3.  **Service Restart**:
    *   Restarted `cdc-cms-service` using `make run`.

## Verification
*   Manually created a connector using `curl` with the updated connection string `mongodb://gpay-mongo:27017/?replicaSet=rs0`.
*   Status confirmed as `RUNNING` for both connector and tasks.
*   Test connector deleted after verification.

## Instructions for User
Please retry creating the connector with the following updated connection string:
- **Old**: `mongodb://localhost:17017/?replicaSet=rs0`
- **New**: `mongodb://gpay-mongo:27017/?replicaSet=rs0`

The system is giờ đã sẵn sàng để xử lý kết nối này.
