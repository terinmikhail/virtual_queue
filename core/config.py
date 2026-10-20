from dataclasses import dataclass


@dataclass(frozen=True)
class GeoPoint:
    lat: float
    lon: float


@dataclass(frozen=True)
class Station:
    uuid: str
    point: GeoPoint


STATIONS: tuple[Station, ...] = (
    Station("S1", GeoPoint(49.1288, 142.6601)),
    Station("S2", GeoPoint(49.1400, 142.6800)),
    Station("S3", GeoPoint(49.1150, 142.6900)),
    Station("S4", GeoPoint(49.1350, 142.6450)),
)
STATIONS_BY_UUID: dict[str, Station] = {s.uuid: s for s in STATIONS}

UNLOAD_POINT = GeoPoint(49.1300, 142.6700)

# 40 машин, по 10 на станцию — привязка фиксирована.
_UNITS_PER_STATION = 10
UNIT_HOME_STATION: dict[str, str] = {}
for _i, _st in enumerate(STATIONS):
    for _j in range(_UNITS_PER_STATION):
        UNIT_HOME_STATION[f"T-{_i * _UNITS_PER_STATION + _j + 1:02d}"] = _st.uuid

# --- Времена и скорости ---------------------------------------------------
SERVICE_SECONDS = 200            # обслуживание
MANEUVER_SECONDS = 30            # манёвр между машинами
OCCUPANCY_SECONDS = SERVICE_SECONDS + MANEUVER_SECONDS   # 230

SPEED_MPS = 10.0                 # 36 км/ч — расчётная скорость
STANDING_SPEED_KMH = 1.0         # порог «машина стоит»

# --- Радиусы --------------------------------------------------------------
STATION_RADIUS_M = 50.0
UNLOAD_RADIUS_M = 50.0
DECISION_RADIUS_M = 1500.0

# --- Горизонты и пороги ---------------------------------------------------
QUEUE_HORIZON_SECONDS = 30 * 60  # 30 минут
FRESHNESS_SECONDS = 30           # позиция старше — исключить из очереди
MIN_GAIN_SECONDS = 60            # минимальный выигрыш для рекомендации