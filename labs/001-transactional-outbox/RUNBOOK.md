# Build → Break → Observe → Fix (PowerShell + psql)

> Run in a disposable local lab. The seed uses fixed UUIDs and is **not repeatable without resetting lab data**. Do not run against a shared database.

## 1. Verify containers and create DB

```powershell
docker ps --format "table {{.Names}}\t{{.Ports}}"
docker exec pg psql -U postgres -c "CREATE DATABASE mbr_outbox_lab;"
docker exec -i pg psql -U postgres -d mbr_outbox_lab < sql/01_schema.sql
```
**PowerShell note:** the last line's `<` redirection is not supported in Windows PowerShell. Use:
```powershell
Get-Content .\sql\01_schema.sql -Raw | docker exec -i pg psql -U postgres -d mbr_outbox_lab
Get-Content .\sql\02_seed.sql -Raw | docker exec -i pg psql -U postgres -d mbr_outbox_lab
```

## 2. Kafka topic (from host PowerShell)

```powershell
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists --topic mbr.order.events --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic mbr.order.events
```

## 3. Create least-privilege relay identity (psql)

```sql
CREATE ROLE shopflow_relay LOGIN;
GRANT CONNECT ON DATABASE mbr_outbox_lab TO shopflow_relay;
GRANT USAGE ON SCHEMA public TO shopflow_relay;
GRANT SELECT ON public.outbox_events TO shopflow_relay;
GRANT UPDATE (published_at) ON public.outbox_events TO shopflow_relay;
```
Set a unique password interactively using `\password shopflow_relay` in psql. Never commit or paste the password.

In PowerShell, from the lab directory:
```powershell
$env:PGPASSWORD = Read-Host "Relay password" -MaskInput
uv add "psycopg[binary]" confluent-kafka
```
If your PowerShell version produces a SecureString instead of plaintext, set `PGPASSWORD` securely using your preferred secret mechanism. Never write credentials to the repo.

## 4. BREAK: crash after Kafka ACK

```powershell
$env:CRASH_AFTER_KAFKA_ACK="1"; uv run python .\scripts\outbox_relay.py
```
Observed in our run: event `2222…` was ACKed at partition 1 offset 0, then process exited deliberately. In psql:
```sql
SELECT event_id,published_at FROM outbox_events ORDER BY created_at,event_id;
```
Both rows remained pending.

## 5. RECOVER: restart and reproduce duplicate

```powershell
Remove-Item Env:CRASH_AFTER_KAFKA_ACK -ErrorAction SilentlyContinue; uv run python .\scripts\outbox_relay.py
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic mbr.order.events --partition 1 --offset 0 --max-messages 2 --timeout-ms 10000
```
In our run, `2222…` appeared at offsets **0 and 1**. Partition/offset values can differ on a fresh environment.

## 6. FIX: idempotent inventory SQL

```sql
-- Run in psql after the seed. New event is inserted only once.
BEGIN;
WITH claimed AS (
 INSERT INTO processed_events(event_id)
 VALUES ('22222222-2222-4222-8222-222222222222')
 ON CONFLICT DO NOTHING RETURNING event_id
)
UPDATE inventory
SET available_quantity=available_quantity-1,reserved_quantity=reserved_quantity+1
WHERE product_id='HEADPHONE-001'
 AND available_quantity>=1 AND EXISTS (SELECT 1 FROM claimed);
COMMIT;
-- Run the same block again: UPDATE 0, no second reservation.
```

## 7. BREAK: out-of-stock marker

```sql
BEGIN;
UPDATE inventory SET available_quantity=0 WHERE product_id='HEADPHONE-001';
WITH claimed AS (
 INSERT INTO processed_events(event_id)
 VALUES ('66666666-6666-4666-8666-666666666666')
 ON CONFLICT DO NOTHING RETURNING event_id
)
UPDATE inventory SET available_quantity=available_quantity-1,reserved_quantity=reserved_quantity+1
WHERE product_id='HEADPHONE-001' AND available_quantity>=1
AND EXISTS (SELECT 1 FROM claimed);
SELECT * FROM processed_events WHERE event_id='66666666-6666-4666-8666-666666666666';
ROLLBACK;
```
Observed: `UPDATE 0` but event marker existed inside the transaction. Rollback removed marker and restored inventory to **9 available / 1 reserved**.

**Production caveat:** the SQL demonstration is not a complete consumer. It must check the reservation result and rollback or persist a terminal business outcome before acknowledging Kafka.
