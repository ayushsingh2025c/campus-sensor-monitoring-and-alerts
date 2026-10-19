# Load Test Report

**Run date:** 2026-10-19
**Target:** 10,000 accepted sensor readings within one second
**Result:** **Not met** in the final simultaneous burst (approximately 1.000418 seconds).

## Diagnosis

The earlier 11.095-second submission path did a persisted-reading database lookup for every item in a batch and invoked Redis Lua once per reading. That meant 10,000 PostgreSQL queries and 10,000 Redis round trips for only 20 HTTP requests. Redis was deliberately configured with AOF `appendfsync=always`, making each Redis write durable before it is acknowledged; this was not disabled.

The original worker then read one stream event at a time, queried rules and the rolling average for that event, inserted the reading, evaluated/upserted alerts, committed, acknowledged, and logged individually. With PostgreSQL `synchronous_commit=on` and `fsync=on`, committing each of 10,000 readings imposed substantial durable-commit overhead. The baseline worker took about 170 seconds to finish draining.

The database did not show evidence of an index or table-scan bottleneck: the reading event ID is the primary key, the event-time query has a `(sensor_id, metric_type, observed_at)` index, and the test database contained only a few existing rows. The SQLAlchemy pool is size 5 with up to 5 overflow connections. The main application-level costs were the per-item query/publish/commit pattern. In the initial optimized concurrent burst, batch request latency rose sharply and missed the target by 30 ms. Host memory was also under pressure, and no per-process CPU profile was collected, so attributing the residual to a single host resource would be speculative.

## Changes made

- Batch ingestion now validates every item as before, performs one PostgreSQL lookup for already-persisted IDs per HTTP batch, and calls a single bounded Redis Lua script per batch. The script atomically checks each event ID, appends new events, records their canonical payloads in the idempotency hash, and returns an outcome per item.
- The worker reads up to 100 events, bulk-inserts them, fetches matching rules once per batch, evaluates rolling windows in event-arrival order against the same event-time intervals, upserts alerts once per triggered rule/sensor pair, and commits once for the batch. It acknowledges the stream messages only after commit and emits one summary log per batch. Duplicate payload checks and active-alert uniqueness remain enforced.
- The simulator has a `--burst` option that starts all bounded batch requests immediately. Without that option it continues to pace work across `--duration`; these are distinct test modes.
- The batch envelope now holds raw JSON items until the route validates each item as a `ReadingEvent`, avoiding a redundant generic `dict[str, object]` traversal while preserving per-item Pydantic validation and partial-success errors.

PostgreSQL and Redis durability were preserved: PostgreSQL reported `fsync=on` and `synchronous_commit=on`; Redis 8.0.5 reported AOF enabled and `appendfsync=always`. No database, ownership, Redis durability, or repository history was reset or changed.

## Environment and setup

- Windows 11, Intel Core i7-12650H (16 logical processors), Python 3.14.4; FastAPI 0.148.0, Uvicorn 0.54.0, SQLAlchemy 2.1.5, Redis Python client 8.1.0.
- Local PostgreSQL 18.6, `shared_buffers=128MB`, pool size 5 plus up to 5 overflow connections.
- Redis 8.0.5 on port 6380. The legacy port 6379 endpoint was not used because it does not support the Redis protocol/Streams version needed by this client.
- One Uvicorn process and one Python worker; batch size 500 at the API and 100 at the worker.
- The first optimized paced run began with approximately 0.51 GiB available memory; monitoring during that run observed a 0.31 GiB minimum. Around the final burst, the host had approximately 0.4 GiB available. System CPU samples around the tests ranged from roughly 18% to 59%; the machine was not isolated from other applications.
- Before the final comparison the host had about 0.51 GiB available RAM; spot CPU samples were 22-32%. No API worker-count increase was attempted under this memory constraint.

API health returned HTTP 200 with PostgreSQL and Redis both `ok`; Alembic reported `0001_initial (head)`.

## Results

All runs used 10,000 unique event IDs, 20 HTTP batch requests of 500 readings, one sensor, a unique metric type, all values set to 100, and a temporary 60-second rolling-average rule (`>` 30). Expected alert behavior was one `OPEN` alert, not one alert per reading.

| Measurement | Previously published baseline | Optimized paced run | Optimized simultaneous burst | Final comparison after removing duplicate input traversal |
| --- | ---: | ---: | ---: | ---: |
| Schedule | paced across 1 second | paced across 1 second | all 20 requests released immediately | all 20 requests released immediately |
| Submitted readings | 10,000 | 10,000 | 10,000 | 10,000 |
| Accepted / duplicate / rejected / failed | 10,000 / 0 / 0 / 0 | 10,000 / 0 / 0 / 0 | 10,000 / 0 / 0 / 0 | 10,000 / 0 / 0 / 0 |
| HTTP failures | 0 | 0 | 0 | 0 | 0 |
| Measured request interval | 11.095 s | 1.035 s | 1.030 s | ~1.000418 s |
| Accepted readings/second | 901.28 | 9,663.11 | 9,713.10 | 9,995.82 |
| Batch request latency p50 / p95 | 7,799.76 / 10,250.07 ms | 90.35 / 134.76 ms | 794.43 / 968.96 ms | 755.67 / 984.40 ms |

The optimized paced run was **10.72×** faster than baseline by measured interval. The final comparison's simulator displayed `measured_burst_seconds` rounded to three decimals (`1.0`) and accepted throughput rounded to two decimals (`9,995.82`). The latter corresponds to approximately **1.000418 seconds**, about 0.42 ms over the limit. Therefore the required target is a **fail**, not a pass. The final input-validation optimization did not safely clear the target.

For the final comparison, the first database check 0.072 seconds after the simulator returned already found 10,000 rows and 10,000 distinct event IDs. There was exactly one `OPEN` alert and zero pending stream messages. The previous optimized simultaneous run had the same persistence and alert results. The persisted result was available before or at the first check, not necessarily exactly at that timestamp.

## Reproduction

Start PostgreSQL and a Redis Streams server with AOF `appendfsync=always`, then start `python -m app.worker` and `python -m uvicorn app.main:app --host 127.0.0.1 --port 8000` in separate terminals. Set `REDIS_URL` to the Redis Streams endpoint in both terminals and the test terminal. Confirm `GET /health` returns `ok`.

Use an unused metric type and create the isolated rule:

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
```

Submit the actual simultaneous burst:

```powershell
$env:REDIS_URL = 'redis://127.0.0.1:6380/0'
python -m app.simulator --base-url http://127.0.0.1:8000 --sensors 1 --types $metricType --rate 10000 --duration 1 --mode batch --batch-size 500 --breach-every 1 --seed 20261019 --workers 20 --burst
```

The simulator reports accepted item throughput from the interval between the first request start and last response completion. HTTP requests/sec is separate from sensor readings/sec. Check PostgreSQL for all 10,000 distinct event IDs, confirm the one expected active alert, and ensure the Redis consumer-group pending count returns to zero. Remove only rows, alert/history, stream entries, and idempotency-hash members that have first been verified against this run's unique IDs and metric type.

## Limitations

The final burst did not meet the threshold, missing by approximately 0.42 ms; this difference is smaller than the precision of the simulator's displayed duration, so the rounded throughput is used to classify the result conservatively. The single Uvicorn process, concurrent request processing, batch-response generation, and constrained host resources remain relevant limits; this session did not profile them individually or test scaled API workers. No further burst or risky tuning run was attempted. A resource-isolated run with higher-resolution timing is needed to assess the remaining sub-millisecond difference reliably.

Benchmark-created readings, the temporary rule and alert/history, and only their exact Redis stream/hash records were removed after verification. Existing readings and the pre-existing Redis entries were preserved.
