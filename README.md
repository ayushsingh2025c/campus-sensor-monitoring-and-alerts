# Campus Sensor Monitoring and Alerts

Backend using the agreed Python, FastAPI, PostgreSQL, SQLAlchemy/Alembic, Redis Streams, and Python worker stack. Phase 1 decisions and contracts are in `docs/phase-1-design.md`, `docs/api-specification.md`, `docs/database-schema.md`, and `docs/acceptance-checklist.md`.

## Prerequisites

- Python 3.14 or newer
- PostgreSQL 18 (or a compatible supported PostgreSQL release), including the `psql` client for the database commands below
- Redis 5 or newer (Redis Streams required); use Redis 6.2+ if relying on `XAUTOCLAIM`, although this worker uses `XPENDING`/`XCLAIM`
- Git
- Docker is not required for the local setup below; Docker Compose remains an option for a future reproducible service bundle.

The application uses PostgreSQL for durable readings/rules/alerts and Redis Streams as the durable ingestion boundary. Run Redis with AOF enabled and `appendfsync always` for this local configuration.

## Fresh checkout and Python environment

From the repository root in PowerShell:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Keep `.env` local and never commit it. Configure `DATABASE_URL` for your PostgreSQL instance. The example uses `sensor_app` / `replace-me` as placeholders; replace them locally and URL-encode special characters in the password. `REDIS_URL` defaults to `redis://127.0.0.1:6380/0`; change it if your Redis Streams server uses a different address. The remaining `REDIS_*` variables configure the stream, consumer group, consumer name, blocking interval, and pending-message reclaim timeout.

## PostgreSQL database and migrations

Create a dedicated application login and database as a PostgreSQL administrator. At a local `psql` prompt, substitute a strong local password:

```powershell
psql -h 127.0.0.1 -U postgres -d postgres -W
```

```sql
CREATE ROLE sensor_app LOGIN PASSWORD 'replace-with-a-strong-local-password';
CREATE DATABASE sensor_monitoring OWNER sensor_app;
```

If the role/database already exist, reuse them rather than recreating them. Set `DATABASE_URL` in `.env` to that database and role, then apply or check migrations:

```powershell
python -m alembic upgrade head
python -m alembic current
```

The initial migration is `0001_initial`. Migrations only target the database selected by `DATABASE_URL`.

## Redis Streams

Run a Redis server that supports Streams. For WSL Ubuntu, an administrator may install the packaged service with:

```bash
sudo apt-get update
sudo apt-get install redis-server
redis-server --bind 127.0.0.1 --port 6380 --appendonly yes --appendfsync always
```

WSL2 localhost forwarding allows Windows clients to reach the loopback-bound Redis server. If the legacy Redis-compatible endpoint on port 6379 does not support Streams, keep the Streams server on port 6380 and set in each PowerShell terminal:

```powershell
$env:REDIS_URL = 'redis://127.0.0.1:6380/0'
```

Verify the Redis version and persistence configuration from a Redis CLI:

```bash
redis-cli -p 6380 INFO server | grep redis_version
redis-cli -p 6380 CONFIG GET appendonly
redis-cli -p 6380 CONFIG GET appendfsync
```

Expected configuration is Redis 5+ with `appendonly=yes` and `appendfsync=always`.

## Start the worker and API

Start each process in its own PowerShell terminal from the repository root, with the virtual environment activated and the same `REDIS_URL` configured.

**Terminal 1 — worker:**

```powershell
.\.venv\Scripts\Activate.ps1
$env:REDIS_URL = 'redis://127.0.0.1:6380/0'
python -m app.worker
```

**Terminal 2 — API:**

```powershell
.\.venv\Scripts\Activate.ps1
$env:REDIS_URL = 'redis://127.0.0.1:6380/0'
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Omit or change the `REDIS_URL` override when `.env` already points to the correct Redis server. Check readiness and interactive API documentation at:

- Health: `http://127.0.0.1:8000/health`
- OpenAPI UI: `http://127.0.0.1:8000/docs`

The health endpoint returns `200` only when PostgreSQL and Redis Streams are available. In a third terminal, verify end-to-end stream-to-worker persistence:

```powershell
.\.venv\Scripts\Activate.ps1
$env:REDIS_URL = 'redis://127.0.0.1:6380/0'
python -m app.smoke_test
```

## Tests and implemented APIs

PostgreSQL-backed tests use the configured database and clean up their own test rows:

```powershell
python -m pytest -q
```

The implemented API includes:

- `POST /api/v1/readings` and partial-success `POST /api/v1/readings/batch` (maximum 500 per request)
- `POST /api/v1/rules` and `GET /api/v1/rules`
- `GET /api/v1/alerts`, `GET /api/v1/alerts/{alertId}`, acknowledge, and resolve
- `GET /health`

Rule creation supports all five comparison operators and optional rolling event-time average windows. The worker persists the reading and evaluates matching rules in the same PostgreSQL transaction, then acknowledges the stream event. Rule edit/enable/disable routes are not implemented yet.

## Sensor simulator

Generate reproducible individual or batch traffic with deliberate threshold breaches. This example schedules 1,000 readings/sec for 10 seconds in 25-reading batches and reports attempted, accepted, duplicate, rejected, failed, request latency, and observed throughput:

```powershell
python -m app.simulator --base-url http://127.0.0.1:8000 --sensors 100 --types temperature,humidity --rate 1000 --duration 10 --mode batch --batch-size 25 --breach-every 10 --seed 1 --workers 32
```

The simulator's accepted readings/sec is computed from accepted item outcomes and the measured interval between the first request start and last request completion. Request throughput is the number of HTTP calls/sec; with batch requests, it is not the same as readings/sec.

## Load tests

Run a configurable Locust batch-ingestion test for a sustained sample:

```powershell
$env:REDIS_URL = 'redis://127.0.0.1:6380/0'
$env:BATCH_SIZE = '25'
$env:SENSOR_COUNT = '100'
$env:METRIC_TYPE = 'temperature'
$env:BREACH_EVERY = '10'
python -m locust -f locustfile.py --headless -H http://127.0.0.1:8000 -u 100 -r 20 -t 30s --only-summary
```

Locust reports HTTP requests/sec and request-latency percentiles. Multiply request throughput by batch size only when all items are accepted; inspect per-item outcomes and persisted database counts to establish actual readings/sec. A configured user count or spawn rate is not an achieved throughput result.

For the controlled **10,000 readings / one-second burst** measurement, use the simulator's 500-item batches (20 concurrent HTTP requests). First create an alert rule for a unique metric type so worker-side rule evaluation is included; select an unused metric type:

```powershell
$metricType = 'load-test-unique'
$rule = @{
    name = 'Load test threshold'
    sensorId = 'sim-00000'
    type = $metricType
    operator = '>'
    threshold = 30
    windowSeconds = 60
    aggregation = 'average'
    enabled = $true
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/v1/rules' -ContentType 'application/json' -Body $rule
$env:REDIS_URL = 'redis://127.0.0.1:6380/0'
python -m app.simulator --base-url http://127.0.0.1:8000 --sensors 1 --types $metricType --rate 10000 --duration 1 --mode batch --batch-size 500 --breach-every 1 --seed 2026 --workers 20 --burst
```

`--burst` releases the 20 requests immediately rather than pacing them across one second. The simulator reports accepted sensor readings/sec from the measured first-request-start to last-response-completion interval; HTTP request throughput is not the same metric. Verify PostgreSQL contains 10,000 distinct event IDs, the expected single active alert exists, and the Redis consumer group has no pending messages. The report in `docs/load-test-report.md` records the measured baseline and optimized runs: the latest immediate burst achieved 9,995.82 readings/sec (approximately 1.000418 seconds), so the one-second target was not met.
