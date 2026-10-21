import argparse
import json
import logging
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
        units_state_topic: str = "units_state.v1"
    ):
        self.log = logging.getLogger("telemetry")
        self.in_topic = in_topic
        self.unit_events_topic = unit_events_topic
        self.station_events_topic = station_events_topic
        self.units_state_topic = units_state_topic
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.decision_consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": "p1-decisions",  # своя группа
            "auto.offset.reset": "latest",
        })
        self.decision_consumer.subscribe(["decision.v1"])
        self.producer = Producer({"bootstrap.servers": bootstrap})

        self.state: dict[str, UnitState] = {}
        self.now_ts: int = 0
        self.running = True
        self.last_snapshot_ts = 0

    def run(self) -> None:
        self.consumer.subscribe([self.in_topic])
        print(f"P1 started: {self.in_topic} -> "
              f"{self.unit_events_topic}, {self.station_events_topic}")

        try:
            while self.running:
                while True:
                    dmsg = self.decision_consumer.poll(0)
                    if dmsg is None or dmsg.error():
                        break
                    self._handle_decision(dmsg)
                    self.decision_consumer.commit(message=dmsg)
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}", file=sys.stderr)
                    continue
                self._handle(msg)
        finally:
            self.consumer.close()
            self.decision_consumer.close()
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
        new_state, events = apply_telemetry(prev, payload, log=self.log)
        self.state[unit_uuid] = new_state

        for ev in events:
            self._publish_event(ev)
        if self.now_ts - self.last_snapshot_ts >= 1:
            self._publish_snapshot()
            self.last_snapshot_ts = self.now_ts

        self.consumer.commit(message=kafka_msg)

    def _handle_decision(self, kafka_msg):
        try:
            data = json.loads(kafka_msg.value())
        except json.JSONDecodeError:
            return

        if data["result"] != "recommended":
            return

        uuid = data["unit_uuid"]
        to_station = data["to_station"]

        # обновляем target_station в своём state
        state = self.state.get(uuid)
        if state is not None:
            state.target_station = to_station
            self.log.info(f"P1: Unit {uuid} target -> {to_station}")

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

    def _publish_snapshot(self):
        payload = json.dumps({
            uuid: {
                "unit_uuid": state.unit_uuid,
                "last_ts": state.last_ts,
                "point": {"lat": state.point.lat, "lon": state.point.lon},
                "speed_kmh": state.speed_kmh,
                "status": state.status.value,
                "target_station": state.target_station,
                "home_station": state.home_station,
                "decision_made_this_trip": state.decision_made_this_trip,
                "last_decision_ts": state.last_decision_ts,
                "entered_station_radius_ts": state.entered_station_radius_ts,
            }
            for uuid, state in self.state.items()
        }).encode()

        self.producer.produce(
            self.units_state_topic,
            key="all",  # ← один ключ (все машины)
            value=payload,
        )
        self.producer.poll(0)

    def _publish_unit_state(self, state: UnitState) -> None:
        payload = json.dumps({
            "unit_uuid": state.unit_uuid,
            "last_ts": state.last_ts,
            "point": {"lat": state.point.lat, "lon": state.point.lon},
            "speed_kmh": state.speed_kmh,
            "status": state.status.value,
            "target_station": state.target_station,
            "home_station": state.home_station,
            "decision_made_this_trip": state.decision_made_this_trip,
            "last_decision_ts": state.last_decision_ts,
            "entered_station_radius_ts": state.entered_station_radius_ts,
        }).encode()

        self.producer.produce(
            self.units_state_topic,
            key=state.unit_uuid,
            value=payload,
        )
        self.producer.poll(0)

    def stop(self) -> None:
        self.running = False


def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
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
