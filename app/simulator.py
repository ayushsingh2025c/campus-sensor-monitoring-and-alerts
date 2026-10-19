import argparse
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from math import ceil
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def generate_readings(
    count: int,
    *,
    sensor_count: int,
    metric_types: list[str],
    breach_every: int,
    seed: int,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    generated_at = datetime.now(UTC).isoformat()
    return [
        {
            "eventId": str(uuid4()),
            "sensorId": f"sim-{index % sensor_count:05d}",
            "type": metric_types[index % len(metric_types)],
            "value": 100 if breach_every and (index + 1) % breach_every == 0 else round(rng.uniform(15, 25), 3),
            "timestamp": generated_at,
            "metadata": {"source": "simulator", "sequence": index},
        }
        for index in range(count)
    ]


def _chunks(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[start : start + size] for start in range(0, len(items), size)]


def _scheduled_times(
    started_at: float,
    job_count: int,
    chunk_size: int,
    rate: float,
    *,
    burst: bool,
) -> list[float]:
    if burst:
        return [started_at] * job_count
    return [started_at + index * chunk_size / rate for index in range(job_count)]


def _submit(base_url: str, items: list[dict[str, Any]], mode: str, scheduled_at: float) -> dict[str, Any]:
    delay = scheduled_at - time.monotonic()
    if delay > 0:
        time.sleep(delay)
    endpoint = "/api/v1/readings" if mode == "individual" else "/api/v1/readings/batch"
    body: object = items[0] if mode == "individual" else {"readings": items}
    request = Request(
        f"{base_url.rstrip('/')}{endpoint}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started_at = time.monotonic()
    try:
        with urlopen(request, timeout=30) as response:
            result = json.loads(response.read())
            if mode == "individual":
                key = "duplicate" if result.get("status") == "duplicate" else "accepted"
                counts = {"accepted": 0, "duplicate": 0, "rejected": 0}
                counts[key] = 1
            else:
                counts = {"accepted": 0, "duplicate": 0, "rejected": 0}
                for outcome in result.get("results", []):
                    counts[outcome.get("status", "rejected")] += 1
            return {
                **counts,
                "http_failures": 0,
                "failed_readings": 0,
                "latency_ms": (time.monotonic() - started_at) * 1000,
                "request_started_at": started_at,
                "request_finished_at": time.monotonic(),
            }
    except HTTPError as exc:
        if exc.code == 207 and mode == "batch":
            result = json.loads(exc.read())
            counts = {"accepted": 0, "duplicate": 0, "rejected": 0}
            for outcome in result.get("results", []):
                counts[outcome.get("status", "rejected")] += 1
            http_failures = 0
            failed_readings = 0
        else:
            counts = {"accepted": 0, "duplicate": 0, "rejected": 0}
            http_failures = 1
            failed_readings = len(items)
    except (TimeoutError, URLError, OSError, json.JSONDecodeError):
        counts = {"accepted": 0, "duplicate": 0, "rejected": 0}
        http_failures = 0
        failed_readings = len(items)
    finished_at = time.monotonic()
    return {
        **counts,
        "http_failures": http_failures,
        "failed_readings": failed_readings,
        "latency_ms": (finished_at - started_at) * 1000,
        "request_started_at": started_at,
        "request_finished_at": finished_at,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate campus sensor readings for the API")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--sensors", type=int, default=100)
    parser.add_argument("--types", default="temperature,humidity")
    parser.add_argument("--rate", type=float, default=100, help="Target readings per second")
    parser.add_argument("--duration", type=float, default=10, help="Run duration in seconds")
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--mode", choices=("individual", "batch"), default="individual")
    parser.add_argument(
        "--burst",
        action="store_true",
        help="start all generated requests immediately instead of pacing them across the duration",
    )
    parser.add_argument("--breach-every", type=int, default=10, help="Set every Nth reading to 100; 0 disables")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--workers", type=int, default=32)
    args = parser.parse_args()
    if args.sensors < 1 or args.rate <= 0 or args.duration <= 0 or args.batch_size < 1 or args.workers < 1:
        parser.error("sensors, rate, duration, batch-size, and workers must be positive")
    if args.breach_every < 0:
        parser.error("breach-every must be nonnegative")
    metric_types = [value.strip() for value in args.types.split(",") if value.strip()]
    if not metric_types:
        parser.error("types must include at least one metric type")

    target_count = max(1, ceil(args.rate * args.duration))
    readings = generate_readings(
        target_count,
        sensor_count=args.sensors,
        metric_types=metric_types,
        breach_every=args.breach_every,
        seed=args.seed,
    )
    chunk_size = 1 if args.mode == "individual" else args.batch_size
    jobs = _chunks(readings, chunk_size)
    started = time.monotonic()
    counts = {
        "attempted": target_count,
        "accepted": 0,
        "duplicate": 0,
        "rejected": 0,
        "http_failures": 0,
        "failed_readings": 0,
    }
    request_latencies: list[float] = []
    request_started: list[float] = []
    request_finished: list[float] = []
    scheduled_times = _scheduled_times(started, len(jobs), chunk_size, args.rate, burst=args.burst)
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                _submit,
                args.base_url,
                items,
                args.mode,
                scheduled_times[index],
            )
            for index, items in enumerate(jobs)
        ]
        for future in as_completed(futures):
            result = future.result()
            request_latencies.append(result.pop("latency_ms"))
            request_started.append(result.pop("request_started_at"))
            request_finished.append(result.pop("request_finished_at"))
            for key, value in result.items():
                if key in {"accepted", "duplicate", "rejected", "http_failures", "failed_readings"}:
                    counts[key] += value
    elapsed = time.monotonic() - started
    burst_seconds = (
        max(request_finished) - min(request_started)
        if request_started and request_finished
        else 0
    )
    result = {
        **counts,
        "requests": len(jobs),
        "batch_size": chunk_size,
        "mode": args.mode,
        "schedule_mode": "burst" if args.burst else "paced",
        "run_elapsed_seconds": elapsed,
        "measured_burst_seconds": burst_seconds,
        "accepted_readings_per_second": round(counts["accepted"] / burst_seconds, 2) if burst_seconds else 0,
        "request_latency_ms": {
            "p50": round(_percentile(request_latencies, 0.50), 2) if request_latencies else None,
            "p95": round(_percentile(request_latencies, 0.95), 2) if request_latencies else None,
            "max": round(max(request_latencies), 2) if request_latencies else None,
        },
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
