from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from core.apply_telemetry import apply_telemetry
from core.config import STATIONS, GeoPoint
from core.decide import decide
from core.models import (
    Decision, DecisionResult, QueueEntry,
    StationState, UnitEventType, UnitState, UnitStatus,
)


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _serialize_decision(d: Decision) -> bytes:
    payload: dict = {
        "unit_uuid": d.unit_uuid,
        "at": _iso(d.at),
        "result": d.result.value,
    }
    if d.result == DecisionResult.RECOMMENDED:
        payload["from_station"] = d.from_station
        payload["to_station"] = d.to_station
        payload["gain_seconds"] = d.gain_seconds
    else:
        payload["reason"] = d.reason.value if d.reason else None
    return json.dumps(payload).encode()


def _deserialize_station_snapshot(raw: bytes) -> StationState:
    """Восстановить StationState из снимка station_q.v1 (queue.v1-формат)."""
    data = json.loads(raw)
    queue = [
        QueueEntry(
            unit_uuid=e["unit_uuid"],
            eta=e["eta"],
            service_start=e["service_start"],
            free_at=e["free_at"],
            wait_seconds=e["wait_seconds"],
        )
        for e in data.get("queue", [])
    ]
    return StationState(
        uuid=data["station_uuid"],
        occupied_by=None,          # из снимка не восстанавливаем (не нужно для decide)
        occupied_until=0,
        queue=queue,
    )


class P3Recommender:
    def __init__(
        self,
        *,
        bootstrap: str = "localhost:9092",
        group_id: str = "p3-recommender",
        unit_events_topic: str = "unit_events.v1",
        station_q_topic: str = "station_q.v1",
        decision_topic: str = "decision.v1",
        station_snapshot_group_id: str | None = None,
    ):
        self.log = logging.getLogger("decide")
        self.unit_events_topic = unit_events_topic
        self.station_q_topic = station_q_topic
        self.decision_topic = decision_topic

        # 1. основной consumer: unit_events.v1 (свои партиции)
        self.events_consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

        # 2. broadcast consumer: station_q.v1 (все партиции)
        #    group_id уникален на инстанс -> каждый читает ВСЕ партиции
        if station_snapshot_group_id is None:
            import uuid
            station_snapshot_group_id = f"p3-stations-{uuid.uuid4()}"
        self.stations_consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": station_snapshot_group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        self.units_consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": "p3-units-state",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.units_consumer.subscribe(["units_state.v1"])

        self.producer = Producer({"bootstrap.servers": bootstrap})

        self.units: dict[str, UnitState] = {}
        self.stations: dict[str, StationState] = {
            s.uuid: StationState(uuid=s.uuid, occupied_by=None, occupied_until=0)
            for s in STATIONS
        }
        self.now_ts: int = 0
        self.running = True

    def run(self) -> None:
        self.events_consumer.subscribe([self.unit_events_topic])
        self.stations_consumer.subscribe([self.station_q_topic])
        print(f"P3 started: {self.unit_events_topic} + {self.station_q_topic} "
              f"-> {self.decision_topic}")

        try:
            while self.running:
                # 1. telemetry -> units
                max_batch = 100
                count = 0
                while count < max_batch:
                    umsg = self.units_consumer.poll(0)
                    if umsg is None or umsg.error():
                        break
                    self._handle_units_state(umsg)
                    self.units_consumer.commit(message=umsg)
                    count += 1
                # 2. снимки станций
                smsg = self.stations_consumer.poll(0)
                if smsg is not None and not smsg.error():
                    self._handle_station_snapshot(smsg)
                    self.stations_consumer.commit(message=smsg)

                # 3. события машин
                emsg = self.events_consumer.poll(0.5)
                if emsg is None:
                    continue
                if emsg.error():
                    print(f"Consumer error: {emsg.error()}", file=sys.stderr)
                    continue
                self._handle_event(emsg)
        finally:
            self.events_consumer.close()
            self.units_consumer.close()
            self.stations_consumer.close()
            self.producer.flush()
            print("P3 stopped.")

    def _handle_station_snapshot(self, kafka_msg) -> None:
        try:
            st = _deserialize_station_snapshot(kafka_msg.value())
            self.stations[st.uuid] = st
        except Exception as e:
            print(f"Bad station snapshot: {e}", file=sys.stderr)

    def _handle_units_state(self, kafka_msg):
        try:
            data = json.loads(kafka_msg.value())
        except json.JSONDecodeError:
            return

        # пропускаем старые (по одной машине)
        if "unit_uuid" in data:
            return

        for uuid, state_data in data.items():
            self.units[uuid] = UnitState(
                unit_uuid=state_data["unit_uuid"],
                last_ts=state_data["last_ts"],
                point=GeoPoint(state_data["point"]["lat"], state_data["point"]["lon"]),
                speed_kmh=state_data["speed_kmh"],
                status=UnitStatus(state_data["status"]),
                target_station=state_data["target_station"],
                home_station=state_data["home_station"],
                decision_made_this_trip=state_data["decision_made_this_trip"],
                last_decision_ts=state_data["last_decision_ts"],
                entered_station_radius_ts=state_data["entered_station_radius_ts"],
            )

    def _handle_event(self, kafka_msg) -> None:
        try:
            event = json.loads(kafka_msg.value())
        except json.JSONDecodeError as e:
            print(f"Bad JSON: {e}", file=sys.stderr)
            self.events_consumer.commit(message=kafka_msg)
            return

        ts = int(event["ts"])
        self.now_ts = max(self.now_ts, ts)

        # только CROSSED_DECISION_RADIUS -> решение
        if event["type"] != UnitEventType.CROSSED_DECISION_RADIUS.value:
            self.events_consumer.commit(message=kafka_msg)
            return

        unit_uuid = event["unit_uuid"]
        unit = self.units.get(unit_uuid)
        if unit is None:
            # нет состояния — не можем решать
            print(f"No UnitState for {unit_uuid}, skip", file=sys.stderr)
            self.events_consumer.commit(message=kafka_msg)
            return

        # ядро: решение
        decision = decide(unit, self.stations, self.now_ts, event_ts=ts, log=self.log)
        if decision.unit_uuid == "T-01":
            self.log.debug(f"Decision = {decision.result}, station = {decision.to_station}")
        # публикация
        self._publish_decision(decision)

        # commit
        self.events_consumer.commit(message=kafka_msg)

    def _publish_decision(self, d: Decision) -> None:
        self.producer.produce(
            self.decision_topic,
            key=d.unit_uuid,
            value=_serialize_decision(d),
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
    p = P3Recommender(bootstrap=args.bootstrap)

    def _stop(signum, frame):
        p.stop()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    p.run()


if __name__ == "__main__":
    main()
