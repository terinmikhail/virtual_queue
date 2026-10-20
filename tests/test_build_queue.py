"""Тесты P2: build_queue (очередь, горизонт, stale) и handle_event."""
from core.config import OCCUPANCY_SECONDS, QUEUE_HORIZON_SECONDS
from core.models import StationState, UnitEventType, UnitStatus
from core.build_queue import build_queue, handle_event
from tests.conftest import make_unit, ts, point_at_distance_from


# --- 1. Занявшая станцию ---

def test_occupied_first_with_eta_none():
    """Занявшая — первая, eta=None, wait=0."""
    station = StationState(uuid="S1", occupied_by="T-01",
                           occupied_until=ts(12, 2, 50), occupied_since=ts(11, 59))
    queue = build_queue(station, units={}, now=ts(12, 0))
    assert len(queue) == 1
    assert queue[0].unit_uuid == "T-01"
    assert queue[0].eta is None
    assert queue[0].wait_seconds == 0


def test_service_start_uses_occupied_since_not_until_minus_230():
    """БАГ 1: service_start = occupied_since, не occupied_until - 230 (досрочный выезд)."""
    # заняла 11:59, выехала 12:01 (досрочно)
    station = StationState(uuid="S1", occupied_by="T-01",
                           occupied_until=ts(12, 1, 0),       # факт
                           occupied_since=ts(11, 59, 0))       # факт
    queue = build_queue(station, units={}, now=ts(12, 1, 30))
    assert queue[0].service_start == ts(11, 59, 0)
    assert queue[0].free_at == ts(12, 1, 0)


# --- 2. Свободная станция + едущие ---

def test_free_station_first_arrival_no_wait():
    """Свободная станция: первая едущая начинает сразу (wait=0)."""
    station = StationState(uuid="S1", occupied_by=None, occupied_until=0)
    units = {
        "T-01": make_unit("T-01", status=UnitStatus.TO_STATION, target="S1",
                          point=GeoPoint_near_S1(dist_m=3000), last_ts=ts(12, 0)),
    }
    queue = build_queue(station, units, now=ts(12, 0))
    assert len(queue) == 1
    assert queue[0].wait_seconds == 0
    assert queue[0].eta is not None


def test_occupied_station_second_waits():
    """Занятая станция: едущая ждёт до occupied_until."""
    station = StationState(uuid="S1", occupied_by="T-01",
                           occupied_until=ts(12, 2, 50), occupied_since=ts(11, 59))
    units = {
        "T-02": make_unit("T-02", status=UnitStatus.TO_STATION, target="S1",
                          point=point_at_distance_from(S1_POINT, dist_m=1200), last_ts=ts(12, 0)),
    }
    queue = build_queue(station, units, now=ts(12, 0))
    # T-01 + T-02
    assert queue[0].unit_uuid == "T-01"
    assert queue[1].unit_uuid == "T-02"
    assert queue[1].service_start == ts(12, 2, 50)   # ждёт T-01
    assert queue[1].wait_seconds > 0


# Ждущая в радиусе

def test_waiting_in_radius_in_queue_with_entered_ts():
    """Ждущая в радиусе (AT_STATION, не занявшая) — в очереди с eta = entered_ts."""
    station = StationState(uuid="S1", occupied_by="T-01",
                           occupied_until=ts(12, 2, 50), occupied_since=ts(11, 59))
    units = {
        "T-02": make_unit("T-02", status=UnitStatus.AT_STATION, target="S1",
                          last_ts=ts(12, 0), entered_radius=ts(11, 58)),
    }
    queue = build_queue(station, units, now=ts(12, 0))
    assert queue[1].unit_uuid == "T-02"
    assert queue[1].eta == ts(11, 58)                # момент входа
    assert queue[1].wait_seconds == (ts(12, 2, 50) - ts(11, 58))   # 290


def test_waiting_in_radius_stale_excluded():
    """ждущая stale (last_ts старше 30 с) — НЕ в очереди."""
    station = StationState(uuid="S1", occupied_by="T-01",
                           occupied_until=ts(12, 2, 50), occupied_since=ts(11, 59))
    units = {
        "T-02": make_unit("T-02", status=UnitStatus.AT_STATION, target="S1",
                          last_ts=ts(11, 58),            # stale относительно now=12:00
                          entered_radius=ts(11, 58)),
    }
    queue = build_queue(station, units, now=ts(12, 0))
    # только T-01, T-02 исключена как stale
    assert [e.unit_uuid for e in queue] == ["T-01"]


# --- 4. Горизонт

def test_eta_equal_horizon_included():
    """eta == horizon -> в очереди (граница включающая)."""
    station = StationState(uuid="S1", occupied_by=None, occupied_until=0)
    now = ts(12, 0)
    horizon = now + QUEUE_HORIZON_SECONDS
    # точка ровно на расстоянии, дающем eta == horizon
    point = point_at_distance_from(S1_POINT, dist_m=SPEED_MPS * QUEUE_HORIZON_SECONDS)
    units = {"T-01": make_unit("T-01", status=UnitStatus.TO_STATION, target="S1",
                               point=point, last_ts=now)}
    queue = build_queue(station, units, now)
    assert len(queue) == 1
    assert queue[0].eta == horizon


def test_eta_beyond_horizon_excluded():
    """eta > horizon -> НЕ в очереди."""
    station = StationState(uuid="S1", occupied_by=None, occupied_until=0)
    now = ts(12, 0)
    point = point_at_distance_from(S1_POINT, dist_m=SPEED_MPS * (QUEUE_HORIZON_SECONDS + 60))
    units = {"T-01": make_unit("T-01", status=UnitStatus.TO_STATION, target="S1",
                               point=point, last_ts=now)}
    queue = build_queue(station, units, now)
    assert queue == []


# --- 5. Фильтры ---

def test_unit_targeting_other_station_excluded():
    """Машина едет к S2 -> не в очереди S1."""
    station = StationState(uuid="S1", occupied_by=None, occupied_until=0)
    units = {"T-01": make_unit("T-01", status=UnitStatus.TO_STATION, target="S2",
                               last_ts=ts(12, 0))}
    queue = build_queue(station, units, now=ts(12, 0))
    assert queue == []


def test_stale_transit_excluded():
    """Stale едущая -> не в очереди."""
    station = StationState(uuid="S1", occupied_by=None, occupied_until=0)
    units = {"T-01": make_unit("T-01", status=UnitStatus.TO_STATION, target="S1",
                               last_ts=ts(11, 50))}    # stale
    queue = build_queue(station, units, now=ts(12, 0))
    assert queue == []


# --- 6. handle_event ---

def test_handle_arrived_fills_occupied():
    station = StationState(uuid="S1", occupied_by=None, occupied_until=0)
    event = {"type": UnitEventType.ARRIVED_AT_STATION.value,
             "unit_uuid": "T-01", "ts": ts(12, 0), "target_station": "S1"}
    handle_event(station, event, {}, ts(12, 0))
    assert station.occupied_by == "T-01"
    assert station.occupied_since == ts(12, 0)
    assert station.occupied_until == ts(12, 0) + OCCUPANCY_SECONDS


def test_handle_arrived_when_occupied_does_nothing():
    station = StationState(uuid="S1", occupied_by="T-00",
                           occupied_until=ts(12, 2, 50), occupied_since=ts(11, 59))
    event = {"type": UnitEventType.ARRIVED_AT_STATION.value,
             "unit_uuid": "T-01", "ts": ts(12, 0), "target_station": "S1"}
    handle_event(station, event, {}, ts(12, 0))
    assert station.occupied_by == "T-00"   # не изменилось


def test_handle_left_station_by_occupant():
    station = StationState(uuid="S1", occupied_by="T-01",
                           occupied_until=ts(12, 2, 50), occupied_since=ts(11, 59))
    event = {"type": UnitEventType.LEFT_STATION.value,
             "unit_uuid": "T-01", "ts": ts(12, 1), "target_station": "S1"}
    handle_event(station, event, {}, ts(12, 1))
    assert station.occupied_by is None
    assert station.occupied_since is None
    assert station.occupied_until == ts(12, 1)   # факт


def test_handle_left_station_by_non_occupant_noop():
    station = StationState(uuid="S1", occupied_by="T-00",
                           occupied_until=ts(12, 2, 50), occupied_since=ts(11, 59))
    event = {"type": UnitEventType.LEFT_STATION.value,
             "unit_uuid": "T-01", "ts": ts(12, 1), "target_station": "S1"}
    handle_event(station, event, {}, ts(12, 1))
    assert station.occupied_by == "T-00"   # не изменилось


# --- helpers ---

from core.config import GeoPoint, SPEED_MPS, STATIONS_BY_UUID

S1_POINT = STATIONS_BY_UUID["S1"].point


def GeoPoint_near_S1(dist_m: float) -> GeoPoint:
    point = point_at_distance_from(S1_POINT, dist_m=SPEED_MPS * QUEUE_HORIZON_SECONDS)
    return point
