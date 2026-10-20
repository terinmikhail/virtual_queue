from dataclasses import dataclass, field
from enum import Enum

from core.config import GeoPoint


class UnitStatus(str, Enum):
    AT_STATION = "at_station"
    TO_UNLOAD = "to_unload"
    TO_STATION = "to_station"


# Состояние машины (P1)
@dataclass
class UnitState:
    unit_uuid: str
    last_ts: int
    point: GeoPoint
    speed_kmh: float
    status: UnitStatus
    target_station: str            # своя или рекомендованная
    home_station: str              # своя (не меняется)
    decision_made_this_trip: bool = False
    last_decision_ts: int | None = None
    entered_station_radius_ts: int | None = None


# Очередь (P2)
@dataclass
class QueueEntry:
    unit_uuid: str
    eta: int | None                # None - машина на станции
    service_start: int             # Время, когда машина начала свое обслуживание
    free_at: int                   # время, когда машина освободит станцию
    wait_seconds: int


@dataclass
class StationState:
    uuid: str
    occupied_by: str | None                                # None - станция свободна
    occupied_until: int                                    # до какого момента станция занята
    occupied_since: int | None = None                      # c какого момента станция занята
    queue: list[QueueEntry] = field(default_factory=list)  # список очереди
    last_published_hash: str | None = None                 # кэш для отслеживания изменения очереди SHA-256


# --- Внутренние события (P1 -> P2/P3) ------------------------------------
class UnitEventType(str, Enum):
    ARRIVED_AT_STATION = "arrived_at_station"
    LEFT_STATION = "left_station"
    LEFT_UNLOAD = "left_unload"
    CROSSED_DECISION_RADIUS = "crossed_decision_radius"
    EN_ROUTE_TO_STATION = "en_route_to_station"


@dataclass
class UnitEvent:
    unit_uuid: str
    ts: int
    type: UnitEventType
    target_station: str            # к какой станции относится событие


class DecisionResult(str, Enum):
    RECOMMENDED = "recommended"
    REJECTED = "rejected"


class RejectReason(str, Enum):
    NO_GAIN = "no_gain"
    STALE_TELEMETRY = "stale_telemetry"


@dataclass
class Decision:
    unit_uuid: str
    at: int
    result: DecisionResult
    from_station: str | None = None
    to_station: str | None = None
    gain_seconds: int | None = None
    reason: RejectReason | None = None