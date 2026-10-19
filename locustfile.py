import os
from datetime import UTC, datetime
from uuid import uuid4

from locust import HttpUser, between, task


class SensorIngestionUser(HttpUser):
    wait_time = between(0, 0.01)

    def on_start(self) -> None:
        self.batch_size = max(1, int(os.getenv("BATCH_SIZE", "1")))
        self.sensor_count = max(1, int(os.getenv("SENSOR_COUNT", "100")))
        self.metric_type = os.getenv("METRIC_TYPE", "temperature")
        self.breach_every = max(0, int(os.getenv("BREACH_EVERY", "10")))
        self.sequence = 0

    @task
    def submit_batch(self) -> None:
        timestamp = datetime.now(UTC).isoformat()
        readings = []
        for _ in range(self.batch_size):
            self.sequence += 1
            readings.append(
                {
                    "eventId": str(uuid4()),
                    "sensorId": f"load-{self.sequence % self.sensor_count:05d}",
                    "type": self.metric_type,
                    "value": (
                        100
                        if self.breach_every and self.sequence % self.breach_every == 0
                        else 20 + (self.sequence % 10) / 10
                    ),
                    "timestamp": timestamp,
                    "metadata": {"source": "locust"},
                }
            )
        self.client.post("/api/v1/readings/batch", json={"readings": readings}, name="/api/v1/readings/batch")
