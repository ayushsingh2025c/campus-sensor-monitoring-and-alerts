# Phase 1 Design

## Scope and technology

Build a single-repository backend using the agreed Python, FastAPI, PostgreSQL, SQLAlchemy/Alembic, Redis Streams, Python worker, pytest, Locust, and Docker Compose stack. Docker is not currently available on the implementation machine; local development will use its existing PostgreSQL service and Redis in WSL Ubuntu. No dashboard, additional queue, or alternate database is in scope.

## Reading and ingestion decisions

- A reading has a client-supplied UUID `eventId`, non-empty `sensorId` and `type`, finite numeric `value` with at most six fractional digits, timezone-aware ISO-8601 `timestamp`, and optional JSON-object `metadata`. Store timestamps in UTC; reject timestamps more than five minutes in the future. `type` is an exact-match string rather than an assumed fixed metric allow-list.
- Validate each batch item independently. The initial batch limit is 500 items; return an outcome for every item: accepted, duplicate, or rejected with a stable error code and message. A malformed or oversized batch envelope is rejected as a whole.
- The API reports acceptance only after an atomic Redis Lua operation confirms `XADD` and records the event ID. Configure Redis AOF persistence with `appendfsync always` for the initial durability boundary; a publish failure is not accepted.
- `eventId` is the idempotency key. Replaying the same event is a duplicate and is not evaluated twice. Reuse of an ID with different content is a conflict. The stream-side idempotency hash must be retained with the stream.
- Persist accepted events before acknowledging their stream messages. A processing failure leaves the message pending for retry; the unique event ID makes redelivery safe.

## Rules, windows, and alerts

- A rule has an ID, name, optional sensor filter, metric type, comparison operator (`>`, `>=`, `<`, `<=`, `==`), numeric threshold, enabled flag, and optional rolling-window duration. Window rules use the arithmetic average.
- Windows are rolling event-time intervals `(reading_timestamp - duration, reading_timestamp]`, anchored at each reading being evaluated. Readings are retained even when late. An out-of-order reading evaluates its own historical window, including older readings in that interval and excluding newer readings; readings older than the window do not contribute. Alert timestamps record when the worker evaluates the match.
- Repeated matches update the existing active alert for the same rule and sensor instead of creating another. A partial unique database index enforces at most one `OPEN` or `ACKNOWLEDGED` alert for that pair. A later match after resolution may create a new alert.
- The only status transitions are `OPEN -> ACKNOWLEDGED -> RESOLVED`. Persist transition timestamps and lifecycle history. Invalid or repeated transitions are rejected.

## Data flow and verification

FastAPI validates requests and atomically publishes/idempotency-registers readings in Redis Streams. A consumer-group worker persists readings to PostgreSQL and acknowledges a message only after commit. On restart, the worker reclaims idle pending messages with `XPENDING`/`XCLAIM` so Redis 5+ supports retries. Rule evaluation and alert creation use the same database transaction as reading persistence. The initial service health endpoint checks PostgreSQL and Redis Streams support.

Verify the contract with API validation and per-item batch tests, migration tests on PostgreSQL, idempotent worker redelivery tests, window-boundary/late-data tests, active-alert uniqueness and transition tests, and an end-to-end stream-to-database smoke test. Phase 1 acceptance items are in [the checklist](acceptance-checklist.md); endpoint and schema details are in [the API specification](api-specification.md) and [schema draft](database-schema.md).
