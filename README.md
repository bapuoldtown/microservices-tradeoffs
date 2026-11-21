# Microservices Trade-offs — Mission Battle Ready

Hands-on distributed-systems engineering: **build → break → observe → fix → explain the trade-offs**.

## Labs

### [001 — ShopFlow Transactional Outbox](labs/001-transactional-outbox/README.md)
**The order that never reached the warehouse.** PostgreSQL + Kafka + Python. Reproduce the crash-after-ACK window, duplicate publication, idempotent inventory reservation, and atomic rollback.

![ShopFlow architecture](labs/001-transactional-outbox/architecture/shopflow-outbox.svg)

> These are educational experiments, not a production-ready outbox/consumer implementation.
