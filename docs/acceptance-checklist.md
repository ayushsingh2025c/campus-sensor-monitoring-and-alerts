# Acceptance Checklist

## Phase 1 design decisions

- [x] Reading fields, validation expectations, UTC timestamp handling, and metric-type semantics are defined.
- [x] Batches define per-item outcomes and whole-request envelope errors.
- [x] The ingestion durability boundary and event-ID idempotency behavior are defined.
- [x] Rule operators, optional rolling-average windows, and enabled state are defined.
- [x] Late/out-of-order reading behavior and active-alert deduplication are defined.
- [x] Alert transitions, API endpoints, schema, and transaction boundaries are documented.

## Runnable foundation

- [x] FastAPI starts from documented configuration.
- [x] `/health` checks PostgreSQL and Redis Streams compatibility.
- [x] Alembic applies the initial schema to PostgreSQL.
- [x] Redis Streams accepts a test event with AOF enabled and `appendfsync always`.
- [x] Worker consumes real stream events, persists them, then acknowledges after commit.
- [x] Replaying the event ID does not create a second reading or alert.
- [x] Unit, PostgreSQL, and Redis Streams integration checks pass.

## Core behavior for later implementation

- [x] Individual and batch ingestion return the documented outcomes and durability errors.
- [x] Rule creation/listing, filters, pagination, and enabled/disabled status work.
- [x] Single-reading and rolling-average rules produce expected alerts, including late/out-of-order input.
- [x] At most one active alert exists per rule/sensor pair; repeated event delivery is idempotent.
- [x] Alert lifecycle permits only `OPEN -> ACKNOWLEDGED -> RESOLVED` and stores timestamps/history.
- [x] Sensor simulator and Locust load-test scenario are implemented and smoke-tested.
