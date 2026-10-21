import argparse
import json
import logging
import random
import signal
import time
from dataclasses import dataclass

from confluent_kafka import Consumer

from core.config import UNIT_HOME_STATION, STATIONS_BY_UUID, GeoPoint, STATIONS, UNLOAD_POINT
from core.geo import distance_m
from core.models import UnitStatus
from generator.func import random_point_on_ground, step, is_point_inside_any_station, \
    point_on_station_boundary_facing_unload, is_point_inside_any_load
from generator.kafka_io import TelemetryPublisher


@dataclass
class VirtualUnit:
    unit_uuid: str
    status: UnitStatus
    target_station: str
    lat: float
    lon: float
    speed: float
    now_ts: int


@dataclass
class VirtualStation:
    uuid: str
    occupied_by: str | None
    occupied_since: int | None
    occupied_until: int | None
    queue: list


class TelemetryGenerator:
    def __init__(self, start_time: int, dt: float, speed: int = 1, bootstrap: str = "localhost:9092",
                 publisher: TelemetryPublisher | None = None):
        self.log = logging.getLogger("simulate")
        self.units: dict[str, VirtualUnit] = {}
        self.stations: dict[str, VirtualStation] = {}
        self.now_ts = start_time
        self.create_units()
        self.create_station()
        self.create_random_point()
        self.dt = dt
        self.speed = speed
        self.publisher = publisher  # None — без Kafka (тесты)
        self.running = True
        self.decision_consumer = Consumer({
            "bootstrap.servers": bootstrap,  # или передать в конструктор
            "group.id": "generator-decisions",
            "auto.offset.reset": "latest",
        })
        self.decision_consumer.subscribe(["decision.v1"])

    def simulate_move(self):
        while self.running:
            dmsg = self.decision_consumer.poll(0)
            if dmsg is not None and not dmsg.error():
                data = json.loads(dmsg.value())
                if data["result"] == "recommended":
                    uuid = data["unit_uuid"]
                    to_station = data["to_station"]
                    if uuid in self.units:
                        self.units[uuid].target_station = to_station
            self.now_ts += self.dt
            for uuid, v_u in self.units.items():
                if is_point_inside_any_station(
                        p=GeoPoint(v_u.lat, v_u.lon),
                        station_uuid=v_u.target_station
                ) and uuid not in self.stations[v_u.target_station].queue and v_u.status == UnitStatus.TO_STATION:
                    if uuid == "T-01":
                        self.log.debug(f"Unit {uuid} at station!")
                    self.update_state_unit(
                        uuid_unit=uuid,
                        point=STATIONS_BY_UUID[v_u.target_station].point,
                        speed=0,
                        status=UnitStatus.AT_STATION,
                        now_ts=self.now_ts,
                        target_station=v_u.target_station
                    )
                    self.add_unit_to_queue(uuid_unit=uuid, station_id=v_u.target_station)
                if is_point_inside_any_load(p=GeoPoint(v_u.lat, v_u.lon)) and v_u.status == UnitStatus.TO_UNLOAD:
                    if uuid == "T-01":
                        self.log.debug(f"Unit {uuid} at load, go to station!")
                    self.update_state_unit(
                        uuid_unit=uuid,
                        point=GeoPoint(v_u.lat, v_u.lon),
                        speed=10,
                        status=UnitStatus.TO_STATION,
                        now_ts=self.now_ts,
                        target_station=v_u.target_station
                    )
                if v_u.status != UnitStatus.AT_STATION:
                    if v_u.status == UnitStatus.TO_STATION:
                        end_point = STATIONS_BY_UUID[v_u.target_station].point
                    else:
                        end_point = UNLOAD_POINT
                    new_point = step(
                        now_ts=v_u.now_ts,
                        speed=10,
                        point=GeoPoint(v_u.lat, v_u.lon),
                        end_point=end_point,
                        dt=int(self.now_ts - v_u.now_ts)
                    )
                    if uuid == "T-01":
                        distance = distance_m(GeoPoint(v_u.lat, v_u.lon), end_point)
                        self.log.debug(
                            f"Unit {uuid} point: {new_point} status: {v_u.status.value} target_st: {v_u.target_station} distance: {distance}")
                    v_u.lat = new_point[1].lat
                    v_u.lon = new_point[1].lon
                    v_u.now_ts = new_point[0]
            for id_station, v_s in self.stations.items():
                if not v_s.occupied_by:
                    if v_s.queue:
                        if v_s.queue[0] == "T-01":
                            self.log.debug(f"Unit {v_s.queue[0]} start loading")
                        self.start_process_load_in_station(
                            now_ts=self.now_ts,
                            uuid_unit=v_s.queue[0],
                            station_uuid=id_station
                        )
                else:
                    if "T-01" in v_s.queue:
                        self.log.debug(f"Unit in queue index - {v_s.queue.index('T-01')}")
                    if self.now_ts > v_s.occupied_until:
                        new_p = point_on_station_boundary_facing_unload(station_uuid=id_station)
                        self.update_state_unit(
                            uuid_unit=v_s.queue[0],
                            point=new_p,
                            speed=10,
                            status=UnitStatus.TO_UNLOAD,
                            now_ts=self.now_ts,
                            target_station=UNIT_HOME_STATION[v_s.queue[0]]
                        )
                        self.delete_unit_to_queue(v_s.queue[0], id_station)
            if self.publisher is not None:
                self.publisher.publish(self.units, self.now_ts)
            if self.speed:
                time.sleep(self.dt / self.speed)

    def add_unit_to_queue(self, uuid_unit: str, station_id: str):
        if uuid_unit == "T-01":
            self.log.debug(f"Unit {uuid_unit} append in queue, len = {len(self.stations[station_id].queue)}")
        self.stations[station_id].queue.append(uuid_unit)

    def delete_unit_to_queue(self, uuid_unit: str, station_id: str):
        if uuid_unit in self.stations[station_id].queue:
            if uuid_unit == "T-01":
                self.log.debug(f"Unit {uuid_unit} delete in queue, len = {len(self.stations[station_id].queue)}")
            self.stations[station_id].queue.pop(self.stations[station_id].queue.index(uuid_unit))
            self.stations[station_id].occupied_by = None

    def update_state_unit(self, uuid_unit: str, point: GeoPoint, speed: int, status: UnitStatus, now_ts: int,
                          target_station: str):
        self.units[uuid_unit].lon = point.lon
        self.units[uuid_unit].lat = point.lat
        self.units[uuid_unit].speed = speed
        self.units[uuid_unit].status = status
        self.units[uuid_unit].now_ts = now_ts
        if target_station:
            self.units[uuid_unit].target_station = target_station
        if uuid_unit == "T-01":
            self.log.debug(f"New status unit: status = {self.units[uuid_unit].status},"
                           f"speed = {self.units[uuid_unit].speed},"
                           f"now_ts = {self.units[uuid_unit].now_ts},"
                           f"target_station = {self.units[uuid_unit].target_station}")

    def start_process_load_in_station(self, now_ts: int, uuid_unit: str, station_uuid: str):
        self.stations[station_uuid].occupied_by = uuid_unit
        ts_load = random.randint(180, 230)
        start_load = now_ts
        end_load = now_ts + ts_load
        self.stations[station_uuid].occupied_since = start_load
        self.stations[station_uuid].occupied_until = end_load
        if uuid_unit == "T-01":
            self.log.debug(f"Start process load in station. Start - {start_load}, End = {end_load}")

    def create_units(self):
        for unit_uuid, home in UNIT_HOME_STATION.items():
            st = STATIONS_BY_UUID[home]
            self.units[unit_uuid] = VirtualUnit(
                unit_uuid=unit_uuid,
                target_station=home,
                status=UnitStatus.AT_STATION,
                lat=st.point.lat,
                lon=st.point.lon,
                speed=0,
                now_ts=int(self.now_ts)
            )

    def create_random_point(self):
        for unit_uuid, v_u in self.units.items():
            random_geo_point = random_point_on_ground()
            v_u.lat = random_geo_point.lat
            v_u.lon = random_geo_point.lon
            if is_point_inside_any_station(p=GeoPoint(v_u.lat, v_u.lon), station_uuid=v_u.target_station):
                self.units[unit_uuid].status = UnitStatus.AT_STATION
            else:
                val = random.choice([UnitStatus.TO_STATION, UnitStatus.TO_UNLOAD])
                self.units[unit_uuid].status = val
            self.log.debug(f"Create. Unit - {unit_uuid} status - {self.units[unit_uuid].status.value}")

    def create_station(self):
        for s in STATIONS:
            self.stations[s.uuid] = VirtualStation(
                uuid=s.uuid,
                occupied_by=None,
                occupied_since=None,
                occupied_until=None,
                queue=[]
            )

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
    ap.add_argument("--speed", type=float, default=1.0)
    ap.add_argument("--dup", type=float, default=0.0)
    ap.add_argument("--late-ms", type=int, default=0)
    args = ap.parse_args()
    publisher = TelemetryPublisher(
        bootstrap=args.bootstrap,
        dup_rate=args.dup,
        late_ms=args.late_ms,
    )
    p = TelemetryGenerator(start_time=int(time.time()), dt=1, speed=10, publisher=publisher, bootstrap=args.bootstrap)

    def _stop(signum, frame):
        p.stop()
        publisher.flush()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    p.simulate_move()


if __name__ == "__main__":
    main()
