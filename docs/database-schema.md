# Database Schema Draft

PostgreSQL is the source of truth for persisted readings, rules, alerts, and alert lifecycle history. IDs are UUIDs; timestamps are timezone-aware UTC. The Alembic initial migration creates this foundation schema.

## Tables

### `readings`

| Column | Type | Constraints / purpose |
|---|---|---|
| `event_id` | UUID | Primary key; client idempotency key |
| `sensor_id` | VARCHAR(128) | Required |
| `metric_type` | VARCHAR(64) | Required exact-match type |
| `value` | NUMERIC(20, 6) | Required finite numeric reading |
| `observed_at` | TIMESTAMPTZ | Required sensor event time |
| `metadata` | JSONB | Optional JSON object |
| `received_at` | TIMESTAMPTZ | Required ingestion/worker persistence time |

Indexes: `(sensor_id, metric_type, observed_at)` and `(metric_type, observed_at)`.

### `alert_rules`

| Column | Type | Constraints / purpose |
|---|---|---|
| `id` | UUID | Primary key |
| `name` | VARCHAR(200) | Required |
| `sensor_id` | VARCHAR(128) | Nullable filter for all sensors |
| `metric_type` | VARCHAR(64) | Required |
| `operator` | VARCHAR(2) | One of `>`, `>=`, `<`, `<=`, `==` |
| `threshold` | NUMERIC(20, 6) | Required |
| `window_seconds` | INTEGER | Nullable; positive when present |
| `aggregation` | VARCHAR(16) | `average` when window is present; otherwise null |
| `enabled` | BOOLEAN | Required |
| `created_at`, `updated_at` | TIMESTAMPTZ | Required |

### `alerts`

| Column | Type | Constraints / purpose |
|---|---|---|
| `id` | UUID | Primary key |
| `rule_id` | UUID | Required foreign key to `alert_rules` |
| `sensor_id` | VARCHAR(128) | Required |
| `status` | VARCHAR(16) | `OPEN`, `ACKNOWLEDGED`, or `RESOLVED` |
| `first_triggered_at`, `last_triggered_at` | TIMESTAMPTZ | Required match times |
| `acknowledged_at`, `resolved_at` | TIMESTAMPTZ | Nullable lifecycle times |

Enforce at most one active (`OPEN` or `ACKNOWLEDGED`) alert per `(rule_id, sensor_id)` with a PostgreSQL partial unique index. Add indexes for `(status, last_triggered_at)` and `(sensor_id, status)`.

### `alert_history`

| Column | Type | Constraints / purpose |
|---|---|---|
| `id` | UUID | Primary key |
| `alert_id` | UUID | Required foreign key to `alerts` |
| `from_status` | VARCHAR(16) | Nullable only for the initial `OPEN` event |
| `to_status` | VARCHAR(16) | Required lifecycle status |
| `changed_at` | TIMESTAMPTZ | Required transition time |

## Transaction and idempotency rules

For each stream event, insert the reading by `event_id` and evaluate rules/alerts in the same transaction. A duplicate event ID with identical content is a no-op; an ID with different content is an integrity conflict. Commit PostgreSQL changes before acknowledging the Redis message. A failed transaction leaves the message pending for retry. Database constraints, rather than application-only checks, enforce event uniqueness and active-alert uniqueness.
