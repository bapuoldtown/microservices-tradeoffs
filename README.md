# Microservices Trade-offs — Mission Battle Ready

Hands-on distributed-systems engineering: **build → break → observe → fix → explain the trade-offs**.

## 🎮 Play the interactive lesson

[**Save Rahul's Order! — interactive website source**](docs/index.html) · [Enable GitHub Pages](https://github.com/bapuoldtown/microservices-tradeoffs/settings/pages) with **Deploy from a branch → main → /docs** to publish the site. Once enabled, the expected URL is `https://bapuoldtown.github.io/microservices-tradeoffs/` (not verified live yet).

## Labs

### [001 — ShopFlow Transactional Outbox](labs/001-transactional-outbox/README.md)
**The order that never reached the warehouse.** PostgreSQL + Kafka + Python. Reproduce the crash-after-ACK window, duplicate publication, idempotent inventory reservation, and atomic rollback.

![ShopFlow architecture](labs/001-transactional-outbox/architecture/shopflow-outbox.svg)

> These are educational experiments, not a production-ready outbox/consumer implementation.
