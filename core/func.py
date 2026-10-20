from core.config import SPEED_MPS, STATIONS_BY_UUID, UNLOAD_POINT
from core.geo import distance_m
from core.models import (
    StationState, UnitState, UnitStatus,
)


def _eta_to_station(st: UnitState, station_uuid: str, now: int) -> int | None:
    """ETA до станции по правилам ТЗ (to_station / to_unload)."""
    if st.status == UnitStatus.TO_STATION:
        dist = distance_m(st.point, STATIONS_BY_UUID[station_uuid].point)
        return now + int(dist / SPEED_MPS)
    if st.status == UnitStatus.TO_UNLOAD:
        to_unload = distance_m(st.point, UNLOAD_POINT)
        from_unload = distance_m(UNLOAD_POINT, STATIONS_BY_UUID[station_uuid].point)
        return now + int((to_unload + from_unload) / SPEED_MPS)
    return None


def _wait_at_station(
        unit: UnitState,
        station: StationState,
        now: int,
) -> int:
    """Ожидание машины у станции: free_at последней приехавшей раньше - eta, >= 0."""
    if unit.status == UnitStatus.AT_STATION and unit.entered_station_radius_ts is not None:
        eta = unit.entered_station_radius_ts
    else:
        eta = _eta_to_station(unit, station.uuid, now)
    if eta is None:
        eta = now
    free_at = now
    if station.occupied_by:
        free_at = station.occupied_until
    for e in station.queue:
        if e.eta is not None and e.eta < eta:
            free_at = max(free_at, e.free_at)
    return max(0, free_at - eta)


def _closer(unit: UnitState, a: str, b: str) -> bool:
    return distance_m(unit.point, STATIONS_BY_UUID[a].point) < \
        distance_m(unit.point, STATIONS_BY_UUID[b].point)
