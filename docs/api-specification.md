# API Specification

Base path: `/api/v1`. JSON property names use the sensor-facing camelCase spelling shown below. Authentication is not part of the initial scope.

## Health

| Method and path | Purpose | Success |
|---|---|---|
| `GET /health` | Readiness check for PostgreSQL and a Redis version supporting Streams | `200` when both respond; `503` otherwise |

## Readings

Reading body:

```json
{
  "eventId": "8b1e85bc-b34a-4cc4-a10f-cf316616617c",
  "sensorId": "building-a-01",
  "type": "temperature",
  "value": 28.4,
  "timestamp": "2026-10-19T12:00:00Z",
  "metadata": {"floor": 2}
}
```

`metadata` is optional. Timestamps must include a UTC offset; values must be finite. The `type` is a non-empty, exact-match identifier, not a hard-coded metric catalogue.

| Method and path | Purpose | Success and errors |
|---|---|---|
| `POST /readings` | Validate and enqueue one reading | `202` after Redis `XADD`; `200` for an identical replay; `409` if an event ID is reused with different content; `422` for invalid input; `503` if the durable stream write fails |
| `POST /readings/batch` | Independently validate and enqueue each item | `207` with one outcome per input item; each outcome is `accepted`, `duplicate`, or `rejected`; malformed envelope returns `422`; more than 500 items returns `413` |

Batch request and response shapes:

```json
{
  "readings": [
    {"eventId": "8b1e85bc-b34a-4cc4-a10f-cf316616617c", "sensorId": "building-a-01", "type": "temperature", "value": 28.4, "timestamp": "2026-10-19T12:00:00Z"},
    {"eventId": "invalid", "sensorId": "building-a-02", "type": "temperature", "value": 26, "timestamp": "2026-10-19T12:00:00Z"}
  ]
}
```

```json
{
  "results": [
    {"index": 0, "eventId": "8b1e85bc-b34a-4cc4-a10f-cf316616617c", "status": "accepted"},
    {"index": 1, "eventId": null, "status": "rejected", "error": {"code": "invalid_event_id", "message": "eventId must be a UUID"}}
  ]
}
```

## Rules

Rule fields: `name`, optional `sensorId`, `type`, `operator` (`>`, `>=`, `<`, `<=`, `==`), numeric `threshold`, optional positive `windowSeconds`, and `enabled`. Window rules use `aggregation: "average"` over the rolling event-time interval `(reading.timestamp - windowSeconds, reading.timestamp]`; delayed readings are evaluated against their own event-time window. Create defaults to enabled unless the client sends `enabled: false`.

| Method and path | Purpose | Success and errors |
|---|---|---|
| `POST /rules` | Create a rule | `201`; `422` for invalid fields |
| `GET /rules` | List rules; filter by `enabled`, `sensorId`, and `type` | `200`, paginated |

## Alerts

| Method and path | Purpose | Success and errors |
|---|---|---|
| `GET /alerts` | List alerts; filter by `status`, `sensorId`, and `ruleId` | `200`, paginated |
| `GET /alerts/{alertId}` | Read one alert including lifecycle history | `200`; `404` if absent |
| `POST /alerts/{alertId}/acknowledge` | Transition `OPEN` to `ACKNOWLEDGED` | `200`; `404` if absent; `409` for an invalid transition |
| `POST /alerts/{alertId}/resolve` | Transition `ACKNOWLEDGED` to `RESOLVED` | `200`; `404` if absent; `409` for an invalid transition |

List endpoints use bounded `limit`/`offset` pagination, with a default limit of 50 and maximum of 200. Request validation errors use a consistent JSON body with a stable `code` and human-readable `message`. Create/list rules and the listed alert routes are implemented; rule editing and enable/disable operations remain future work.
