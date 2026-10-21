import math
import random

from core.config import GeoPoint, STATIONS, UNLOAD_POINT, STATION_RADIUS_M, UNLOAD_RADIUS_M, STATIONS_BY_UUID
from core.geo import distance_m, within_radius

_ALL_POINTS = [s.point for s in STATIONS] + [UNLOAD_POINT]
_LAT_MIN = min(p.lat for p in _ALL_POINTS)
_LAT_MAX = max(p.lat for p in _ALL_POINTS)
_LON_MIN = min(p.lon for p in _ALL_POINTS)
_LON_MAX = max(p.lon for p in _ALL_POINTS)


def step(now_ts: int, speed: int, point: GeoPoint, end_point: GeoPoint, dt: int = 1) -> tuple[int, GeoPoint]:
    new_ts = now_ts + dt
    d = distance_m(point, end_point)
    if d == 0:
        return new_ts, end_point
    ratio = speed / d
    return new_ts, GeoPoint(
        point.lat + (end_point.lat - point.lat) * ratio,
        point.lon + (end_point.lon - point.lon) * ratio,
    )


def random_point_on_ground(
    margin_m: float = 500.0,
    rng: random.Random | None = None,
) -> GeoPoint:
    rng = rng or random
    # 1° широты ~ 111 320 м; 1° долготы ~ 111 320 * cos(lat)
    lat_margin = margin_m / 111_320.0
    lon_margin = margin_m / (111_320.0 * abs(__import__("math").cos(__import__("math").radians((_LAT_MIN + _LAT_MAX) / 2))))

    lat = rng.uniform(_LAT_MIN + lat_margin, _LAT_MAX - lat_margin)
    lon = rng.uniform(_LON_MIN + lon_margin, _LON_MAX - lon_margin)
    return GeoPoint(lat, lon)


def is_point_inside_any_station(p: GeoPoint, station_uuid: str, margin_m: float = 0.0) -> bool:
    if not station_uuid:
        return False
    st = STATIONS_BY_UUID[station_uuid]
    return within_radius(p, st.point, STATION_RADIUS_M + margin_m)


def is_point_inside_any_load(p: GeoPoint, margin_m: float = 0.0) -> bool:
    up = UNLOAD_POINT
    return within_radius(p, up, UNLOAD_RADIUS_M + margin_m)


def point_on_station_boundary_facing_unload(station_uuid: str) -> GeoPoint:
    """Точка на границе радиуса станции (50 м), со стороны разгрузки."""
    st = STATIONS_BY_UUID[station_uuid]
    # направление от станции к разгрузке
    dlat = UNLOAD_POINT.lat - st.point.lat
    dlon = UNLOAD_POINT.lon - st.point.lon
    total = math.sqrt(dlat * dlat + dlon * dlon)
    if total == 0:
        return GeoPoint(st.point.lat + STATION_RADIUS_M / 111_320.0, st.point.lon)
    # сдвиг на 50 м в сторону разгрузки
    ratio = ((STATION_RADIUS_M+1) / 111_320.0) / total
    return GeoPoint(
        st.point.lat + dlat * ratio,
        st.point.lon + dlon * ratio,
    )
