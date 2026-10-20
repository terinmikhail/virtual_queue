import math

import pytest

from core.config import GeoPoint, Station, UNLOAD_POINT
from core.models import StationState, UnitState, UnitStatus


def ts(h: int, m: int, s: int = 0) -> int:
    """Час:мин:сек -> epoch-секунды (для читаемости)."""
    return h * 3600 + m * 60 + s


@pytest.fixture
def s1() -> Station:
    return Station("S1", GeoPoint(49.1288, 142.6601))


@pytest.fixture
def s2() -> Station:
    return Station("S2", GeoPoint(49.1400, 142.6800))


@pytest.fixture
def empty_station_s1() -> StationState:
    return StationState(uuid="S1", occupied_by=None, occupied_until=0)


@pytest.fixture
def empty_station_s2() -> StationState:
    return StationState(uuid="S2", occupied_by=None, occupied_until=0)


def make_unit(
    uuid: str,
    *,
    status: UnitStatus = UnitStatus.TO_STATION,
    target: str = "S1",
    home: str = "S1",
    point: GeoPoint | None = None,
    last_ts: int = 0,
    entered_radius: int | None = None,
    decision_made: bool = False,
) -> UnitState:
    """Создать UnitState с разумными дефолтами для тестов."""
    return UnitState(
        unit_uuid=uuid,
        last_ts=last_ts,
        point=point or GeoPoint(49.1288, 142.6601),
        speed_kmh=0.0 if status == UnitStatus.AT_STATION else 20.0,
        status=status,
        target_station=target,
        home_station=home,
        decision_made_this_trip=decision_made,
        entered_station_radius_ts=entered_radius,
    )


def point_at_distance_from(point: GeoPoint, dist_m: float) -> GeoPoint:
    """Точка на расстоянии dist_m от point (обратная haversine, север)."""
    R = 6_371_000.0
    lat1 = math.radians(point.lat)
    lon1 = math.radians(point.lon)
    d = dist_m / R
    lat2 = math.asin(math.sin(lat1) * math.cos(d) +
                     math.cos(lat1) * math.sin(d) * math.cos(0))   # bearing = 0 (север)
    lon2 = lon1 + math.atan2(0.0, math.cos(d) - math.sin(lat1) * math.sin(lat2))
    return GeoPoint(math.degrees(lat2), math.degrees(lon2))

