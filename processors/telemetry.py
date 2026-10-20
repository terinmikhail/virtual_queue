import argparse
import json
import signal
import sys

from confluent_kafka import Consumer, Producer

from core.apply_telemetry import apply_telemetry
from core.models import UnitEvent, UnitState


class P1TelemetryProcessor:
    def __init__(
        self,
        *,
        bootstrap: str = "localhost:9092",
        group_id: str = "p1-telemetry",
        in_topic: str = "telemetry.v1",
        unit_events_topic: str = "unit_events.v1",
        station_events_topic: str = "station_events.v1",
    ):
        self.in_topic = in_topic
        self.unit_events_topic = unit_events_topic
        self.station_events_topic = station_events_topic

        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.producer = Producer({"bootstrap.servers": bootstrap})

        self.state: dict[str, UnitState] = {}
        self.now_ts: int = 0
        self.running = True

    def run(self) -> None:
        self.consumer.subscribe([self.in_topic])
        print(f"P1 started: {self.in_topic} -> "
              f"{self.unit_events_topic}, {self.station_events_topic}")

        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}", file=sys.stderr)
                    continue
                self._handle(msg)
        finally:
            self.consumer.close()
            self.producer.flush()
            print("P1 stopped.")

    def _handle(self, kafka_msg) -> None:
        try:
            payload = json.loads(kafka_msg.value())
        except json.JSONDecodeError as e:
            print(f"Bad JSON: {e}", file=sys.stderr)
            self.consumer.commit(message=kafka_msg)
            return

        unit_uuid = payload["unit_uuid"]
        ts = int(payload["ts"])

        self.now_ts = max(self.now_ts, ts)

        prev = self.state.get(unit_uuid)
        new_state, events = apply_telemetry(prev, payload)
        self.state[unit_uuid] = new_state

        for ev in events:
            self._publish_event(ev)

        self.consumer.commit(message=kafka_msg)

    def _publish_event(self, ev: UnitEvent) -> None:
        payload = json.dumps({
            "unit_uuid": ev.unit_uuid,
            "ts": ev.ts,
            "type": ev.type.value,
            "target_station": ev.target_station,
        }).encode()

        self.producer.produce(
            self.unit_events_topic,
            key=ev.unit_uuid,
            value=payload,
        )
        self.producer.produce(
            self.station_events_topic,
            key=ev.target_station,
            value=payload,
        )
        self.producer.poll(0)

    def stop(self) -> None:
        self.running = False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    args = ap.parse_args()

    p = P1TelemetryProcessor(bootstrap=args.bootstrap)

    def _stop(signum, frame):
        p.stop()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    p.run()


if __name__ == "__main__":
    main()
