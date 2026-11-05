# Handoff: eventsource-rs

Last updated: 2026-11-05. Branch `main` is the source of truth.

## 1. Where things stand

eventsource-rs is a Rust port of eventsource-py, built as a workspace of
crates with hexagonal boundaries: `eventsource-core` defines the domain and
ports and imports no adapter; each adapter crate implements ports.

Merged on `main`:

| Area | Crate(s) | Status |
|---|---|---|
| Domain, decider, aggregates, repository | eventsource-core | done |
| In-memory store, bus, checkpoints, snapshots, DLQ, outbox, locks, read models | eventsource-memory | done |
| SQLite store and all ports | eventsource-sqlite | done |
| PostgreSQL store and all ports | eventsource-postgres | done |
| Redis publisher and stream reader | eventsource-redis | done |
| RabbitMQ publisher and consumer | eventsource-rabbitmq | done |
| Kafka publisher and manual-commit consumer | eventsource-kafka | done |
| Snapshots (policy, repository opt-in) | core, memory, sqlite | done |
| Dead-letter queue and retry | core, memory, sqlite, postgres | done |
| Multi-tenancy (tenant-scoped repositories) | core, memory, sqlite | done |
| Testing toolkit (scenario, harness, recording bus, builder) | eventsource-testkit | done |
| Conformance suites, shared fixtures | eventsource-testing | done |
| Tracer port and tracing adapter | core, eventsource-otel | done |
| Subscription manager (pause, resume, shutdown, health) | core | merged |
| Read-model query, soft delete, versioned writes | core, memory, sqlite, postgres | merged |
| Coordination (leader election, rendezvous ownership) | core, memory, sqlite | merged |
| Review fixes (non_exhaustive errors, wire-decode tests, poison-tolerant locks, weak assertions, shared fixtures, timing) | various | merged |

Verified on `main`: `cargo test --workspace --no-fail-fast` passes
(273 tests) with Redis, Kafka, RabbitMQ and PostgreSQL running.
`cargo clippy --workspace --all-targets` is clean.

## 2. Remaining work

Ordered by value. Items 1 and 2 need a decision first.

1. **Migration subsystem: decision needed.** eventsource-py has about 10k
   lines of live store migration (bulk copy, dual-write, consistency checks,
   position mapping, cutover, rollback) across
   `application/migration/` (59 files), `ports/migration/` (4 files), and
   migration SQL under `adapters/sql/schemas/`. Not started. Either build it
   in stages (position mapping and consistency checker first) or document it
   as out of scope in the README.
2. **Subscription operations gaps** versus Python (`application/subscriptions/`):
   filtering, flow control, circuit breaker, metrics, and a coordinator that
   uses the coordination module. Leader election and rendezvous assignment
   exist but are not wired into the manager.
3. **Read-model querying gaps:** equality filters only (no ne, gt, in);
   ordering by id only; no SQL-side query for SQLite or Postgres (default
   scan loads the whole model); no schema generation or reconcile.
4. **SQLite blocking I/O.** `eventsource-sqlite` runs synchronous rusqlite
   calls inside async trait methods behind `std::sync::Mutex`. The mutex is
   never held across `.await`, but WAL fsyncs and the 5 s busy timeout can
   stall an executor thread. Fix: `spawn_blocking`, or document the adapter
   as blocking.
5. **Shared SQL adapter helper crate.** About 350–450 lines are duplicated
   between `eventsource-sqlite` and `eventsource-postgres` (row decoding,
   column lists, corruption errors, registry checks, dead-letter, lock,
   outbox, snapshot logic). Candidate crate: `eventsource-sql-common`.
6. **Split large types.** `AggregateRepository` (load, snapshot, retry, tenant,
   outbox save) and `SubscriptionRunner` (feed read, checkpoint, retry,
   dead-letter, wake/poll) each mix responsibilities. The in-memory bus also
   mixes registry, dispatch and background facade.
7. **Bus feature depth:** consumer groups and reclaim (Redis), dead-letter
   topics and retry (RabbitMQ, Kafka), stats types, security settings (SASL,
   TLS), and Kafka tenant/category filtering (currently rejected).
8. **Core runtime coupling.** `eventsource-core` depends on tokio (`sync`,
   `time`) because `SubscriptionRunner` uses `watch` and `sleep`. Consider a
   sleeper port if runtime-agnostic core is a goal.
9. **Outbox:** no dead-letter or max-attempts policy; a poison message is
   retried forever. The relay stops at the first failure to preserve order.
10. **Poison messages block consumers** in Redis, RabbitMQ and Kafka: an
    undecodable record fails the read and stays unacknowledged.
11. **Intentionally not ported:** Python's ambient tenant context and
    `handles` decorators (Rust uses an explicit tenant value and explicit
    dispatch); Python's blocking `acquire` on locks (callers poll
    `try_acquire`); Python's serialization and BDD helpers.

## 3. Design decisions to keep

- Core imports no adapter crate and no I/O library. Keep it that way.
- Errors that adapters or users may match on are `#[non_exhaustive]`.
- Snapshot, checkpoint and lock writes never move backwards. Older snapshots
  never replace newer ones. Checkpoint regression is rejected.
- A tenant-scoped repository isolates by namespacing the aggregate category
  (`{tenant}.{Aggregate}`), so isolation holds on the read path, not only the
  feed filter.
- Kafka and Redis positions are scoped to the feed. Checkpoints refer to the
  bus feed, not the source store.
- The SubscriptionRunner owns delivery, retry and checkpointing. Adapters
  do not duplicate retry logic.
- Delivery is at-least-once. Consumers must tolerate duplicates.
- Leadership is computed from the last granted lease and the injected clock.
  `LockStore` has no read operation, so a lost lease is only seen on expiry.
- Ownership uses rendezvous hashing with a hand-rolled, stable hash, so
  assignment does not depend on the Rust version.

## 4. How to run the tests

Tests for the bus and store adapters need live services. Start them first.

| Service | Address | Used by |
|---|---|---|
| Redis | `redis://127.0.0.1:6390` (`REDIS_URL`) | eventsource-redis |
| RabbitMQ | `amqp://guest:guest@127.0.0.1:5672` (`AMQP_URL`) | eventsource-rabbitmq |
| Kafka | `localhost:9092` (`KAFKA_BOOTSTRAP`) | eventsource-kafka |
| PostgreSQL | `postgres://postgres:postgres@127.0.0.1:5432/eventsource_test` (`DATABASE_URL`) | eventsource-postgres |

```sh
redis-server --port 6390 --daemonize yes --save "" --appendonly no
cargo test --workspace --no-fail-fast
cargo clippy --workspace --all-targets
```

Set up on this machine (for a fresh environment):

- PostgreSQL 16 from apt, cluster `16 main` started with `pg_ctlcluster 16 main start`,
  user `postgres` with password `postgres`, database `eventsource_test`.
- RabbitMQ from apt, started with `rabbitmq-server -detached`.
- Kafka 4.1.2 from `downloads.apache.org` (not archive; the proxy blocks
  archive). Config is `config/local-server.properties` with `log.dirs` set
  to a local data directory. Format with `kafka-storage.sh format --standalone`,
  start with `kafka-server-start.sh -daemon`. Check with
  `kafka-topics.sh --bootstrap-server localhost:9092 --list`.

Each test uses random topic, queue, group, or schema names, so runs do not
collide. Leftover resources from older runs are harmless but accumulate:
Postgres test schemas (`t_<uuid>`), RabbitMQ queues (`test-q-*`) and
exchanges (`test-ex-*`), and Kafka topics.

## 5. Known problems

- **Disk space.** Cargo target directories are large (about 13 GB for the
  main clone). Several test runs filled the disk. Delete `target/` in finished
  worktrees before running the full suite.
- **Brokers stop.** The Kafka and RabbitMQ brokers stopped during long runs
  and produced timeouts that look like code failures. Check the ports first.
- **Redis is shared.** Agent workflows shut down the Redis on 6390 when they
  finished. Restart it before running the workspace tests.
- **Postgres collation test.** `ids_order_bytewise_not_by_database_collation`
  passes here because the test database uses `C.UTF-8`, so it cannot detect a
  missing `COLLATE "C"`.
- **Postgres test schemas are never dropped.** Add cleanup before any shared
  CI use.
- **`eventsource-sqlite` Cargo description** is stale ("event store and
  checkpoint store").
- **Stale worktrees.** Agent worktrees are under `/home/user/wt/` and
  `.claude/worktrees/` (ignored by git). Their branches are merged. Delete
  them with `git worktree remove` once you no longer need them.

## 6. Branches

Feature branches were merged into `main` and can be deleted:
`tdd-fix`, `sqlite-adapter`, `snapshots`, `projections-dlq`, `tenancy`,
`testkit`, `telemetry`, `redis-bus`, `locks`, `outbox`, `readmodels`,
`rabbitmq`, `postgres`, `kafka`, `coordination`, `subscriptions`, `hygiene`,
`testhygiene`, `readquery`, `bus-tdd`. The remote branch `add-readme` still
exists because the earlier delete was refused (HTTP 403).

## 7. Working conventions

- Commit directly to `main` when the tests pass.
- Commit messages end with the `Claude-Session:` trailer. No model names in
  commit messages.
- Do not force-push. Ask before deleting remote branches.
- Use TDD: write the failing test, record the failure, then implement.
  Tests that pass on their first run must be checked by mutating the code.
