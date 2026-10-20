import argparse
import random
import signal
import time
from dataclasses import dataclass
from threading import Thread

from core.config import UNIT_HOME_STATION, STATIONS_BY_UUID, GeoPoint, STATIONS
from core.models import UnitStatus, StationState
from generator.func import random_point_on_ground, step, is_point_inside_any_station, \
    point_on_station_boundary_facing_unload
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
    def __init__(self, start_time: int, dt: float, speed: int = 1, publisher: TelemetryPublisher | None = None):
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

    def simulate_move(self):
        while self.running:
            self.now_ts += self.dt

            for uuid, v_u in self.units.items():
                if is_point_inside_any_station(
                        p=GeoPoint(v_u.lat, v_u.lon),
                        station_uuid=v_u.target_station
                ) and uuid not in self.stations[v_u.target_station].queue:
                    self.update_state_unit(
                        uuid_unit=uuid,
                        point=STATIONS_BY_UUID[v_u.target_station].point,
                        speed=0,
                        status=UnitStatus.AT_STATION,
                        now_ts=self.now_ts,
                        target_station=v_u.target_station
                    )
                    self.add_unit_to_queue(uuid_unit=uuid, station_id=v_u.target_station)
                if v_u.status != UnitStatus.AT_STATION:
                    if v_u.target_station:
                        new_point = step(
                            now_ts=v_u.now_ts,
                            speed=10,
                            point=GeoPoint(v_u.lat, v_u.lon),
                            end_point=STATIONS_BY_UUID[v_u.target_station].point,
                            dt=int(self.now_ts - v_u.now_ts)
                        )
                        v_u.lat = new_point[1].lat
                        v_u.lon = new_point[1].lon
                        v_u.now_ts = new_point[0]

            for id_station, v_s in self.stations.items():
                if not v_s.occupied_by:
                    if v_s.queue:
                        self.start_process_load_in_station(
                            now_ts=self.now_ts,
                            uuid_unit=v_s.queue[0],
                            station_uuid=id_station
                        )
                else:
                    if v_s.queue:
                        if self.now_ts > v_s.occupied_until:
                            new_p = point_on_station_boundary_facing_unload(station_uuid=id_station)
                            self.delete_unit_to_queue(v_s.queue[0], id_station)
                            self.update_state_unit(
                                uuid_unit=v_s.queue[0],
                                point=new_p,
                                speed=10,
                                status=UnitStatus.TO_UNLOAD,
                                now_ts=self.now_ts
                            )
            if self.publisher is not None:
                self.publisher.publish(self.units, self.now_ts)
            if self.speed:
                time.sleep(self.dt/self.speed)

    def add_unit_to_queue(self, uuid_unit: str, station_id: str):
        self.stations[station_id].queue.append(uuid_unit)

    def delete_unit_to_queue(self, uuid_unit: str, station_id: str):
        if uuid_unit in self.stations[station_id].queue:
            self.stations[station_id].queue.pop(self.stations[station_id].queue.index(uuid_unit))
            self.stations[station_id].occupied_by = None

    def update_state_unit(self, uuid_unit: str, point: GeoPoint, speed: int, status: UnitStatus, now_ts: int,
                          target_station: str = None):
        self.units[uuid_unit].lon = point.lon
        self.units[uuid_unit].lat = point.lat
        self.units[uuid_unit].speed = speed
        self.units[uuid_unit].status = status
        self.units[uuid_unit].now_ts = now_ts
        if target_station:
            self.units[uuid_unit].target_station = target_station

    def start_process_load_in_station(self, now_ts: int, uuid_unit: str, station_uuid: str):
        self.stations[station_uuid].occupied_by = uuid_unit
        ts_load = random.randint(180, 230)
        start_load = now_ts
        end_load = now_ts+ts_load
        self.stations[station_uuid].occupied_since = start_load
        self.stations[station_uuid].occupied_until = end_load

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
    p = TelemetryGenerator(start_time=int(time.time()), dt=1, speed=1, publisher=publisher)

    def _stop(signum, frame):
        p.stop()
        publisher.flush()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    p.simulate_move()


if __name__ == "__main__":
    main()
