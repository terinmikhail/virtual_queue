import argparse
import hashlib
import json
import signal
import sys
from dataclasses import asdict
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from core.apply_telemetry import apply_telemetry
from core.build_queue import build_queue, handle_event
from core.models import StationState, UnitState


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def hash_queue(queue: list) -> str:
    payload = json.dumps([asdict(e) for e in queue], sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


class P2QueueProcessor:
    def __init__(
        self,
        *,
        bootstrap: str = "localhost:9092",
        group_id: str = "p2-queue",
        in_topic: str = "station_events.v1",
        queue_topic: str = "queue.v1",
        station_q_topic: str = "station_q.v1",
    ):
        self.in_topic = in_topic
        self.queue_topic = queue_topic
        self.station_q_topic = station_q_topic

        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.telemetry_consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": "p2-units",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.telemetry_consumer.subscribe(["telemetry.v1"])
        self.producer = Producer({"bootstrap.servers": bootstrap})

        self.state: dict[str, StationState] = {}
        self.units: dict[str, UnitState] = {}
        self.now_ts: int = 0
        self.running = True

    def run(self) -> None:
        self.consumer.subscribe([self.in_topic])
        print(f"P2 started: {self.in_topic} -> {self.queue_topic}, {self.station_q_topic}")

        try:
            while self.running:
                # 1. telemetry -> units
                tmsg = self.telemetry_consumer.poll(0)
                if tmsg is not None and not tmsg.error():
                    self._handle_telemetry(tmsg)
                    self.telemetry_consumer.commit(message=tmsg)

                # 2. station_events -> очередь
                msg = self.consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}", file=sys.stderr)
                    continue
                self._handle(msg)
        finally:
            self.consumer.close()
            self.telemetry_consumer.close()
            self.producer.flush()
            print("P2 stopped.")

    def _handle(self, kafka_msg) -> None:
        try:
            event = json.loads(kafka_msg.value())
        except json.JSONDecodeError as e:
            print(f"Bad JSON: {e}", file=sys.stderr)
            self.consumer.commit(message=kafka_msg)
            return

        station_uuid = event["target_station"]
        ts = int(event["ts"])
        self.now_ts = max(self.now_ts, ts)

        station = self.state.get(station_uuid) or StationState(
            uuid=station_uuid, occupied_by=None, occupied_until=0,
        )

        # 1. обновить occupied_* по событию (ядро)
        station = handle_event(station, event, self.units, self.now_ts)

        # 2. построить очередь (твоя build_queue)
        new_queue = build_queue(station, self.units, self.now_ts)

        # 3. дедуп: публиковать только если изменилось
        new_hash = hash_queue(new_queue)
        if new_hash != station.last_published_hash:
            self._publish(station, new_queue)
            station.last_published_hash = new_hash

        # 4. сохранить state + commit
        self.state[station_uuid] = station
        self.consumer.commit(message=kafka_msg)

    def _handle_telemetry(self, kafka_msg):
        try:
            payload = json.loads(kafka_msg.value())
        except json.JSONDecodeError as e:
            print(f"Bad telemetry JSON: {e}", file=sys.stderr)
            return
        unit_uuid = payload["unit_uuid"]
        prev = self.units.get(unit_uuid)
        new_state, _ = apply_telemetry(prev, payload)
        self.units[unit_uuid] = new_state

    def _publish(self, station: StationState, queue: list) -> None:
        payload = json.dumps({
            "station_uuid": station.uuid,
            "at": _iso(self.now_ts),
            "queue": [asdict(e) for e in queue],
        }).encode()

        self.producer.produce(self.queue_topic, key=station.uuid, value=payload)
        self.producer.produce(self.station_q_topic, key=station.uuid, value=payload)
        self.producer.poll(0)

    def stop(self) -> None:
        self.running = False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    args = ap.parse_args()
    p = P2QueueProcessor(bootstrap=args.bootstrap)

    def _stop(signum, frame):
        p.stop()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    p.run()


if __name__ == "__main__":
    main()