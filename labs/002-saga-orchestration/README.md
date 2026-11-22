# Mission 002 — Durable Saga orchestration (local executable lab)

**Problem:** Rahul buys headphones for ₹2,500. Inventory is reserved, payment can be declined or its acknowledgement can be lost. How do we complete or compensate safely after retries and restarts?

## What is real vs simulated?

- **Real:** PostgreSQL durable saga state, idempotent checkout keys, operation journal, row locking, SKIP LOCKED worker claims, inventory updates, retry scheduling, and restart recovery.
- **Simulated:** Inventory/payment services share one database and process; the payment provider is an operations table. This **does not prove** cross-service atomicity or external exactly-once payment behavior.
- **Not implemented:** independent service processes, real PSP inquiry/refunds, fencing tokens for external effects, authenticated administration, OpenTelemetry/Grafana, production-grade retry exhaustion and manual intervention.

## Start (Windows WSL2 / Docker)

```bash
cd labs/002-saga-orchestration
docker compose up --build -d
curl -s http://localhost:8802/health
```

Create an order:

```bash
curl -s -X POST http://localhost:8802/checkouts -H 'content-type: application/json' \
  -d '{"request_key":"rahul-001","amount":2500,"fail_mode":"none"}'
```

Copy the `saga.id` UUID from the response:

```bash
curl -s http://localhost:8802/sagas/YOUR_UUID
curl -s http://localhost:8802/inventory
```

Wait 3 seconds for the worker to advance RESERVE → CHARGE → CONFIRM.

## Inject and observe incidents

Use a fresh request key each time:

```bash
# Declined payment → RELEASE compensation → COMPENSATED
curl -s -X POST http://localhost:8802/checkouts -H 'content-type: application/json' \
  -d '{"request_key":"rahul-decline","fail_mode":"decline"}'

# Charge committed but ACK lost → UNCERTAIN → provider inquiry → COMPLETED
curl -s -X POST http://localhost:8802/checkouts -H 'content-type: application/json' \
  -d '{"request_key":"rahul-timeout","fail_mode":"timeout"}'
```

Inspect states while the worker runs:

```bash
docker compose logs -f api
docker compose exec postgres psql -U saga -d shopflow -c \
  'SELECT request_key,state,step,attempts,error FROM sagas ORDER BY created_at;'
docker compose exec postgres psql -U saga -d shopflow -c \
  'SELECT op_key,kind,result FROM operations ORDER BY created_at;'
```

## Restart and verify durability

```bash
docker compose restart api
docker compose exec postgres psql -U saga -d shopflow -c \
  'SELECT request_key,state,step FROM sagas ORDER BY created_at;'
```

The database volume survives API restarts. Workers can reclaim expired leases after a crash.

## Architecture tradeoffs

- A **Saga is not an ACID transaction** spanning remote services. Each service must independently enforce its invariants.
- An idempotency key identifies a business request; a **stable operation key** identifies each external step.
- If payment times out, **inquire before compensating**; otherwise you might cancel an order that was actually charged.
- A successful charge cannot always be undone; real systems need refund workflows and manual escalation.
- This example atomically commits simulated effects and saga transitions only because all effects are local. A genuine multi-service SDK needs durable intent, remote idempotency, and explicit uncertain-outcome handling.
- The worker's lease does not fence external calls. Do not use this code for real payments or production.

## Cleanup

```bash
docker compose down -v
```

This removes the lab database volume and all test state. Do not run against your existing production-like PostgreSQL container.

**Playground:** https://mission-battle-ready-systems-quest.vercel.app/saga-playground.html
