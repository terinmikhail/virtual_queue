import heapq
import json
import random
import time

from confluent_kafka import Producer

from core.models import UnitStatus


class TelemetryPublisher:
    def __init__(self, *, bootstrap: str = "localhost:9092",
                 topic: str = "telemetry.v1",
                 dup_rate: float = 0.0, late_ms: int = 0, seed: int = 42):
        self.topic = topic
        self.dup_rate = dup_rate
        self.late_ms = late_ms
        self.rng = random.Random(seed)
        self.producer = Producer({"bootstrap.servers": bootstrap})
        self.delayed: list[tuple[float, dict]] = []

    def publish(self, units: dict, now_ts: int) -> None:
        for u in units.values():
            msg = {
                "unit_uuid": u.unit_uuid,
                "ts": now_ts,
                "lat": u.lat,
                "lon": u.lon,
                "speed_kmh": 0.0 if u.status == UnitStatus.AT_STATION else 10.0,
            }
            payload = json.dumps(msg)

            if self.dup_rate and self.rng.random() < self.dup_rate:
                self.producer.produce(self.topic, key=u.unit_uuid, value=payload)

            if self.late_ms and self.rng.random() < 0.1:
                delay = self.rng.uniform(0, self.late_ms / 1000.0)
                heapq.heappush(self.delayed, (time.time() + delay, msg))
            else:
                self.producer.produce(self.topic, key=u.unit_uuid, value=payload)

        # отправить созревшие
        now_real = time.time()
        while self.delayed and self.delayed[0][0] <= now_real:
            _, m = heapq.heappop(self.delayed)
            self.producer.produce(self.topic, key=m["unit_uuid"], value=json.dumps(m))

        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush()
