# Load Test Report

**Run date:** 2026-10-19
**Result:** The 10,000-readings-in-one-second target was **not met**.

## Environment and method

- Windows 11 host, Intel Core i7-12650H (16 logical processors), Python 3.14.4, FastAPI 0.148.0, Uvicorn 0.54.0, SQLAlchemy 2.1.5, and Alembic 1.20.0; PostgreSQL 18.6 and Redis 8.0.5 ran locally. The Redis Python client was 8.1.0. Redis Streams persistence was enabled with AOF and `appendfsync=always`.
- FastAPI ran under Uvicorn with one server process; one Python worker consumed the stream. The simulator used 20 client threads.
- A temporary, isolated rule matched sensor `sim-00000` and a unique metric type, using a 60-second rolling average with operator `>` and threshold `30`. All generated readings had value `100`, so the expected outcome was one active alert, not one alert per reading.
- The simulator generated 10,000 unique event IDs as 20 batches of 500, scheduled over one second. The recorded burst duration is the interval from the first HTTP request start to the last response completion. Request latency covers each complete batch request and response; it is not per-reading latency.

The exact simulator command was:

```powershell
$env:REDIS_URL = 'redis://127.0.0.1:6380/0'
python -m app.simulator --base-url http://127.0.0.1:8000 --sensors 1 --types loadtest_10k_f10f3538 --rate 10000 --duration 1 --mode batch --batch-size 500 --breach-every 1 --seed 2026 --workers 20
```

Before the burst, an isolated rule was created through `POST /api/v1/rules` with `sensorId=sim-00000`, `type=loadtest_10k_f10f3538`, `operator=>`, `threshold=30`, `windowSeconds=60`, and `aggregation=average`. The API health endpoint returned HTTP 200 with PostgreSQL and Redis both `ok`.

## Measured results

| Metric | Result |
| --- | ---: |
| Intended readings / scheduling interval | 10,000 / 1 second |
| HTTP batch requests / batch size | 20 / 500 |
| Readings submitted | 10,000 |
| Accepted / duplicate / rejected / failed readings | 10,000 / 0 / 0 / 0 |
| HTTP failures | 0 |
| Measured burst duration | 11.095 seconds |
| Accepted sensor-reading throughput | 901.28 readings/second |
| HTTP request throughput | 1.80 batch requests/second |
| Batch-request latency p50 / p95 / max | 7,799.76 / 10,250.07 / 10,275.35 ms |

The worker had persisted 1,805 readings at the first post-burst check and persisted all 10,000 with zero stream messages pending 170.3 seconds after that check began. PostgreSQL contained 10,000 rows with 10,000 distinct event IDs. Rule evaluation produced exactly one `OPEN` alert and one initial alert-history row, as expected for the de-duplicated active-alert lifecycle.

The target is a **fail**: the burst accepted all intended readings, but took 11.095 measured seconds, or 901.28 accepted readings per second. The 20 HTTP requests/sec schedule and configured simulator rate are not the achieved throughput.

## Limitations and observed bottlenecks

Windows reported approximately 0.38 GiB free physical memory immediately before the burst and 1.30 GiB after it; WSL reported about 7.0 GiB available. This high host memory pressure limits how representative the result is. No CPU utilization profile was collected. The test used only one API process and one serial worker, so it does not characterize a scaled deployment.

In the current implementation, batch ingestion processes each item individually, and the worker persists/evaluates/commits one event at a time. The API burst completed in about 11 seconds, while the worker took substantially longer to drain the queue. Thus HTTP acceptance throughput and end-to-end durable PostgreSQL persistence throughput are distinct; this run does not meet a one-second end-to-end persistence target either.

The temporary rule, its alert/history, and only the readings, Redis event-index entries, and stream messages bearing this run's unique metric/event IDs were removed after verification. Existing readings and the pre-existing Redis stream entries were preserved.
