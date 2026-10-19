# Campus Sensor Monitoring and Alerts

Backend foundation for the agreed Python/FastAPI, PostgreSQL, SQLAlchemy/Alembic, Redis Streams, and Python worker stack. Phase 1 decisions and endpoint/schema contracts are in `docs\phase-1-design.md`, `docs\api-specification.md`, `docs\database-schema.md`, and `docs\acceptance-checklist.md`.

## Local prerequisites

- Python 3.14+
- The existing PostgreSQL 18 Windows service, with an administrator able to create a local application role and database
- WSL2 with Ubuntu and Redis 5.0+ (Streams required); run Redis with AOF enabled for durable ingestion
- No Docker installation is needed for this local path. Docker Compose remains the agreed reproducible deployment option.

The PostgreSQL database `sensor_monitoring` is configured in `.env`, and Alembic revision `0001_initial` has been applied. Ensure the PostgreSQL command-line tools are installed and available on `PATH`.

### Prepare PostgreSQL

Have a PostgreSQL administrator connect locally (the following prompts for the administrator password):

```powershell
psql -h 127.0.0.1 -U postgres -d postgres -W
```

At the `psql` prompt, create a dedicated least-privilege application login and database, substituting a strong local password, only if they do not already exist:

```sql
CREATE ROLE sensor_app LOGIN PASSWORD 'replace-with-a-strong-local-password';
CREATE DATABASE sensor_monitoring OWNER sensor_app;
```

Use that role and password in `.env`. URL-encode special characters in the password. Do not commit `.env`.

### Prepare Redis in WSL Ubuntu

The current `localhost:6379` endpoint reports Redis-compatible version `3.0.504`, which does not support Streams. A Redis 5+ server is required. Redis is not installed as a system service in WSL Ubuntu, and installing it requires interactive `sudo` authorization:

For this workspace, Redis 8.0.5 has been unpacked from the official Ubuntu package repository under the ignored `.tools\redis` directory. Start that local copy without system installation:

```powershell
$repo = (Get-Location).Path
$wslRepo = (wsl -d Ubuntu -- wslpath -a "$repo").Trim()
$redisRoot = "$wslRepo/.tools/redis/root"
$redisData = "$wslRepo/.tools/redis/data"
wsl -d Ubuntu -- sh -lc "mkdir -p '$redisData' && LD_LIBRARY_PATH='$redisRoot/usr/lib/x86_64-linux-gnu' '$redisRoot/usr/bin/redis-server' --bind 127.0.0.1 --port 6380 --protected-mode yes --appendonly yes --appendfsync always --dir '$redisData' --daemonize yes --pidfile '$redisData/redis.pid' --logfile '$redisData/redis.log'"
```

If `.tools\redis` is not present in a fresh checkout, install the system package using the commands below.

```bash
sudo apt-get update
sudo apt-get install redis-server
redis-server --bind 127.0.0.1 --port 6380 --appendonly yes --appendfsync always
```

Keep Redis running in that terminal. AOF with `appendfsync always` is the configured ingestion durability boundary. From Windows, verify Redis Streams support before starting the API or worker:

```bash
redis-cli INFO server | grep redis_version
redis-cli CONFIG GET appendonly
redis-cli CONFIG GET appendfsync
```

## Install and run

From the repository root in PowerShell:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
Copy-Item .env.example .env
```

Edit `.env` with your local PostgreSQL credentials (the current local `.env` already targets `sensor_monitoring`). Apply the schema:

```powershell
python -m alembic upgrade head
```

In separate PowerShell terminals, start the worker and API:

```powershell
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
python -m app.worker
```

Because the existing server uses port 6379, the Redis Streams server above uses port 6380. Set this environment variable in each PowerShell terminal used to run the API, worker, simulator, or smoke test (it overrides `REDIS_URL` in `.env`):

```powershell
$env:REDIS_URL = 'redis://127.0.0.1:6380/0'
```

`GET http://127.0.0.1:8000/health` returns `200` only when PostgreSQL responds and the Redis server supports Streams. With Redis and the worker running, publish a test event and verify that the separate worker consumes, commits, and persists it:

```powershell
python -m app.smoke_test
```

Run local tests with `python -m pytest`. PostgreSQL-backed tests use the configured database and remove only their own generated rows. The smoke test leaves its uniquely identified reading in the database as evidence of successful processing. Readings are limited to six fractional value digits, timestamps more than five minutes ahead are rejected, and the initial batch API limit is 500 items.

## Implemented API

- `POST /api/v1/readings` and `POST /api/v1/readings/batch`
- `POST /api/v1/rules` and `GET /api/v1/rules`
- `GET /api/v1/alerts`, `GET /api/v1/alerts/{alertId}`, acknowledge, and resolve
- `GET /health`

Rule creation supports all five comparison operators and optional rolling average windows. The worker evaluates a reading and persists its alert in the same transaction; it acknowledges stream events only after that transaction commits.

## Simulator and load test

Generate reproducible individual or batch traffic with deliberate threshold breaches:

```powershell
python -m app.simulator --sensors 100 --types temperature,humidity --rate 100 --duration 10 --mode batch --batch-size 25 --breach-every 10 --seed 1
```

Run a basic Locust test:

```powershell
python -m locust -f locustfile.py --headless -H http://127.0.0.1:8000 -u 100 -r 20 -t 30s
```

Set `BATCH_SIZE`, `SENSOR_COUNT`, `METRIC_TYPE`, or `BREACH_EVERY` in the environment to change the Locust request shape. A burst target run can be attempted with `-u 1000 -r 1000 -t 10s`; report actual achieved throughput and errors rather than treating the configured rate as a result.

Rule editing and enable/disable APIs remain future work. The actual end-to-end Redis Streams smoke test and load tests require a compatible Redis 5+ server.
