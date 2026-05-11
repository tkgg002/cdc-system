# Wipe & Bootstrap V2 Runbook

## Mục tiêu

Runbook này dùng cho đợt reset lớn của `centralized-data-service` khi chuyển hẳn sang:

- control plane ở `cdc_system`
- shadow namespace theo `shadow_<source_db>`
- master namespace theo binding/schema đích
- không còn phụ thuộc runtime vào `cdc_internal`

## Tiền đề

1. Đã pull code có migrations tới `038_finalize_cdc_system_namespace.sql`
2. Đã chuẩn bị file seed riêng dựa trên:
   - [bootstrap_cdc_system_v2_template.sql](/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/deployments/sql/bootstrap_cdc_system_v2_template.sql)
   - [bootstrap_cdc_system_v2_local.sql](/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/deployments/sql/bootstrap_cdc_system_v2_local.sql)
3. Đã chốt env:
   - `CDC_SYSTEM_DB_URL`
   - `CDC_SHADOW_DB_URLS`
   - `CDC_MASTER_DB_URLS`
4. Team hiểu rằng:
   - chỉ shadow/master physical tables nằm ngoài `cdc_system`
   - `public` không còn là nơi app để system tables
   - `cdc_internal` phải biến mất sau cutover

## Chuẩn bị seed

Trước khi chạy bootstrap, copy file template:

```bash
cp deployments/sql/bootstrap_cdc_system_v2_template.sql deployments/sql/bootstrap_cdc_system_v2_local.sql
```

Repo hiện cũng đã có sẵn một local seed thực dụng:

- [bootstrap_cdc_system_v2_local.sql](/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/deployments/sql/bootstrap_cdc_system_v2_local.sql)

Flow local mặc định:

- source: `goopay_payment.payments`
- shadow: `shadow_goopay_payment.payments`
- master: `dw_payment.payment_fact`

Sửa các giá trị sau trong file local:

- `connection_code`
- `host`
- `port`
- `default_database`
- `default_schema`
- `secret_ref`
- source database/schema/collection/table thật
- shadow schema theo `shadow_<source_db>`
- master schema/table thật
- mapping rules thật

## Bước 1: Dừng service

Dừng các tiến trình đang ghi dữ liệu:

- `cmd/worker`
- `cmd/sinkworker`
- CMS/API nào đang thao tác metadata

Nếu đang dùng docker compose:

```bash
docker compose stop cdc-worker
```

Nếu có sinkworker chạy riêng, dừng luôn process đó.

## Bước 2: Backup nếu cần

Nếu cần chụp snapshot trước khi wipe:

```bash
docker exec gpay-postgres pg_dump -U user -d goopay_dw > /tmp/goopay_dw_pre_v2_bootstrap.sql
```

## Bước 3: Wipe dữ liệu

Đây là bước do operator quyết định theo môi trường.

Mục tiêu wipe:

- xóa shadow tables cũ
- xóa master tables cũ
- xóa metadata/control tables cũ nếu muốn reset sạch hoàn toàn

Lưu ý:

- không cần cố giữ legacy metadata nếu bạn đã chốt reset-from-scratch
- nếu wipe toàn DB, chỉ cần recreate DB rỗng rồi migrate lại

Repo đã có wipe script tham chiếu:

- [wipe_cdc_runtime_v2.sql](/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/deployments/sql/wipe_cdc_runtime_v2.sql)

Script này sẽ:

1. drop master physical tables từ `cdc_system.master_binding`
2. drop toàn bộ schema `shadow_%`
3. dọn legacy system tables còn sót ở `public`
4. truncate toàn bộ `cdc_system`
5. drop `cdc_internal` nếu còn sót

Chạy ví dụ:

```bash
docker exec -i gpay-postgres psql -v ON_ERROR_STOP=1 -U user -d goopay_dw < deployments/sql/wipe_cdc_runtime_v2.sql
```

## Bước 4: Chạy migrations

Repo hiện đã có target:

```bash
make migrate
```

Target này sẽ apply toàn bộ file `.sql` trong thư mục `migrations/` theo thứ tự tên file.

Nếu muốn chạy tay:

```bash
for file in $(find migrations -maxdepth 1 -type f -name '*.sql' | sort); do
  echo "==> applying $file"
  docker exec -i gpay-postgres psql -v ON_ERROR_STOP=1 -U user -d goopay_dw < "$file"
done
```

Kỳ vọng sau bước này:

- `cdc_system` tồn tại
- các bảng V2 tồn tại
- `cdc_internal` đã bị drop ở migration `038`

## Bước 5: Seed control-plane metadata

Chạy file seed môi trường của bạn:

```bash
docker exec -i gpay-postgres psql -v ON_ERROR_STOP=1 -U user -d goopay_dw < deployments/sql/bootstrap_cdc_system_v2_local.sql
```

Nếu chỉ muốn chạy template demo:

```bash
make migrate-bootstrap
```

Nếu muốn bootstrap local theo flow đã seed sẵn trong repo:

```bash
make migrate-bootstrap-local
```

Lưu ý:

- template mặc định chỉ là flow ví dụ
- production/staging nên dùng file seed môi trường riêng, không dùng template nguyên bản

## Bước 6: Start lại service

Ví dụ:

```bash
docker compose up -d cdc-worker
```

Nếu bạn chạy local:

```bash
go run cmd/worker/main.go
go run cmd/sinkworker/main.go
```

## Bước 7: Verify bootstrap

### 7.1 Verify system tables

Kỳ vọng mọi system table nằm trong `cdc_system`:

```sql
SELECT schemaname, tablename
FROM pg_tables
WHERE schemaname = 'cdc_system'
ORDER BY tablename;
```

Check không còn `cdc_internal`:

```sql
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name = 'cdc_internal';
```

Kỳ vọng: `0 rows`

### 7.2 Verify connections

```sql
SELECT connection_code, role_type, engine_type, status
FROM cdc_system.connection_registry
ORDER BY connection_code;
```

### 7.3 Verify source objects and bindings

```sql
SELECT object_code, source_database, source_schema, source_object_name, profile_status
FROM cdc_system.source_object_registry
ORDER BY object_code;
```

```sql
SELECT binding_code, shadow_schema, shadow_table, physical_table_fqn, ddl_status
FROM cdc_system.shadow_binding
ORDER BY binding_code;
```

```sql
SELECT binding_code, master_schema, master_table, physical_table_fqn, schema_status, is_active
FROM cdc_system.master_binding
ORDER BY binding_code;
```

### 7.4 Verify shadow namespaces

Kỳ vọng shadow table mới nằm ở `shadow_<source_db>`:

```sql
SELECT schemaname, tablename
FROM pg_tables
WHERE schemaname LIKE 'shadow_%'
ORDER BY schemaname, tablename;
```

### 7.5 Verify runtime state

```sql
SELECT runtime_scope, ddl_status, last_success_at, last_error_message
FROM cdc_system.sync_runtime_state
ORDER BY updated_at DESC
LIMIT 20;
```

## Checklist pass/fail

Pass khi:

1. `cdc_internal` không còn tồn tại
2. system tables nằm trong `cdc_system`
3. shadow table được tạo trong `shadow_<source_db>`
4. master table được tạo đúng schema đích
5. ingest ghi được shadow
6. transmute ghi được master
7. `cdc_system.sync_runtime_state` có trạng thái chạy thực

Fail nếu:

1. runtime vẫn log lỗi lookup schema `cdc_internal`
2. system table xuất hiện ở `public`
3. shadow table bị tạo sai schema
4. master binding approved nhưng không auto-create được table

## Ghi chú vận hành

- `Makefile` cũ từng chỉ chạy `001_init_schema.sql`; đã được cập nhật để chạy toàn bộ migrations.
- Template seed không nên dùng nguyên xi cho production.
- `public` schema của Postgres vẫn tồn tại ở mức engine; điều quan trọng là app system tables không còn nằm ở đó.
