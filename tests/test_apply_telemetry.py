"""Тесты P1: apply_telemetry — переходы статусов, события, порядок/дубли."""
from core.config import GeoPoint, STATIONS_BY_UUID
from core.models import UnitEventType, UnitStatus
from core.apply_telemetry import apply_telemetry
from tests.conftest import make_unit, ts


# --- helpers: точки рядом со станцией / разгрузкой ---
S1_POINT = STATIONS_BY_UUID["S1"].point


def _near(p: GeoPoint, dlat: float = 0.0, dlon: float = 0.0) -> GeoPoint:
    return GeoPoint(p.lat + dlat, p.lon + dlon)


# --- 1. Первое сообщение ---
def test_first_message_at_station():
    """Первое сообщение: в радиусе + стоит -> AT_STATION + ARRIVED_AT_STATION."""
    msg = {"unit_uuid": "T-01", "ts": ts(12, 0), "lat": S1_POINT.lat, "lon": S1_POINT.lon, "speed_kmh": 0.0}
    state, events = apply_telemetry(None, msg)
    assert state.status == UnitStatus.AT_STATION
    assert state.entered_station_radius_ts == ts(12, 0)
    assert len(events) == 1
    assert events[0].type == UnitEventType.ARRIVED_AT_STATION


def test_first_message_in_transit():
    """Первое сообщение: далеко, едет -> TO_STATION, без событий."""
    msg = {"unit_uuid": "T-01", "ts": ts(12, 0), "lat": S1_POINT.lat + 0.02, "lon": S1_POINT.lon, "speed_kmh": 20.0}
    state, events = apply_telemetry(None, msg)
    assert state.status == UnitStatus.TO_STATION
    assert state.entered_station_radius_ts is None
    assert events == []


# --- 2. Вход в радиус ---
def test_arrived_at_station_from_transit():
    """TO_STATION -> AT_STATION (вход в радиус + стоит): ARRIVED + entered_radius_ts."""
    prev = make_unit("T-01", status=UnitStatus.TO_STATION, last_ts=ts(11, 59))
    msg = {"unit_uuid": "T-01", "ts": ts(12, 0), "lat": S1_POINT.lat, "lon": S1_POINT.lon, "speed_kmh": 0.0}
    state, events = apply_telemetry(prev, msg)
    assert state.status == UnitStatus.AT_STATION
    assert state.entered_station_radius_ts == ts(12, 0)
    assert [e.type for e in events] == [UnitEventType.ARRIVED_AT_STATION]


def test_repeated_at_station_no_new_event():
    """Повторное AT_STATION -> нет ARRIVED, entered_radius_ts не меняется."""
    prev = make_unit("T-01", status=UnitStatus.AT_STATION, last_ts=ts(11, 59),
                     entered_radius=ts(11, 59))
    msg = {"unit_uuid": "T-01", "ts": ts(12, 0), "lat": S1_POINT.lat, "lon": S1_POINT.lon, "speed_kmh": 0.0}
    state, events = apply_telemetry(prev, msg)
    assert state.status == UnitStatus.AT_STATION
    assert state.entered_station_radius_ts == ts(11, 59)   # не меняется
    assert events == []


# --- 3. Выход из радиуса ---

def test_left_station():
    """AT_STATION -> TO_UNLOAD (выехала): LEFT_STATION + сброс entered_radius_ts."""
    prev = make_unit("T-01", status=UnitStatus.AT_STATION, last_ts=ts(11, 59),
                     entered_radius=ts(11, 59))
    msg = {"unit_uuid": "T-01", "ts": ts(12, 0), "lat": S1_POINT.lat + 0.02, "lon": S1_POINT.lon, "speed_kmh": 20.0}
    state, events = apply_telemetry(prev, msg)
    assert state.status == UnitStatus.TO_UNLOAD
    assert state.entered_station_radius_ts is None
    assert [e.type for e in events] == [UnitEventType.LEFT_STATION]


# --- 4. Разгрузка ---

def test_entered_unload_no_event():
    """TO_UNLOAD + вошла в радиус разгрузки -> остаётся TO_UNLOAD, без событий."""
    from core.config import UNLOAD_POINT
    prev = make_unit("T-01", status=UnitStatus.TO_UNLOAD, last_ts=ts(11, 59))
    msg = {"unit_uuid": "T-01", "ts": ts(12, 0),
           "lat": UNLOAD_POINT.lat, "lon": UNLOAD_POINT.lon, "speed_kmh": 20.0}
    state, events = apply_telemetry(prev, msg)
    assert state.status == UnitStatus.TO_UNLOAD
    assert events == []


def test_left_unload_resets_target_to_home():
    """TO_UNLOAD + вышла из радиуса -> TO_STATION, LEFT_UNLOAD, target = home (БАГ 2)."""
    from core.config import UNLOAD_POINT
    prev = make_unit("T-01", status=UnitStatus.TO_UNLOAD, last_ts=ts(11, 59),
                     target="S2", home="S1")   # была на рекомендованной S2
    msg = {"unit_uuid": "T-01", "ts": ts(12, 0),
           "lat": UNLOAD_POINT.lat + 0.05, "lon": UNLOAD_POINT.lon, "speed_kmh": 20.0}
    state, events = apply_telemetry(prev, msg)
    assert state.status == UnitStatus.TO_STATION
    assert state.target_station == "S1"          # сброс на свою
    assert [e.type for e in events] == [UnitEventType.LEFT_UNLOAD]
    assert events[0].target_station == "S1"


# --- 5. Пересечение радиуса решения ---

def test_crossed_decision_radius_once_per_trip():
    """Впервые < 1500 м до своей -> CROSSED + decision_made. Повторно -> нет."""
    prev = make_unit("T-01", status=UnitStatus.TO_STATION, last_ts=ts(11, 59))
    # точка ~1000 м от S1
    near = GeoPoint(S1_POINT.lat + 0.009, S1_POINT.lon)
    msg1 = {"unit_uuid": "T-01", "ts": ts(12, 0), "lat": near.lat, "lon": near.lon, "speed_kmh": 20.0}
    state1, events1 = apply_telemetry(prev, msg1)
    assert state1.decision_made_this_trip is True
    assert [e.type for e in events1] == [UnitEventType.CROSSED_DECISION_RADIUS]

    # ещё ближе — не должно быть повторного
    near2 = GeoPoint(S1_POINT.lat + 0.005, S1_POINT.lon)
    msg2 = {"unit_uuid": "T-01", "ts": ts(12, 1), "lat": near2.lat, "lon": near2.lon, "speed_kmh": 20.0}
    state2, events2 = apply_telemetry(state1, msg2)
    assert events2 == []


def test_decision_reset_on_arrival():
    """После ARRIVED decision_made сбрасывается (следующий заезд — снова можно)."""
    prev = make_unit("T-01", status=UnitStatus.TO_STATION, last_ts=ts(11, 59),
                     decision_made=True)
    msg = {"unit_uuid": "T-01", "ts": ts(12, 0), "lat": S1_POINT.lat, "lon": S1_POINT.lon, "speed_kmh": 0.0}
    state, events = apply_telemetry(prev, msg)
    assert state.status == UnitStatus.AT_STATION
    assert state.decision_made_this_trip is False


# --- 6. Порядок / дубли ---

def test_duplicate_ignored():
    """ts <= last_ts -> игнор (возврат prev, [])."""
    prev = make_unit("T-01", last_ts=ts(12, 0))
    msg = {"unit_uuid": "T-01", "ts": ts(12, 0), "lat": 0.0, "lon": 0.0, "speed_kmh": 0.0}
    state, events = apply_telemetry(prev, msg)
    assert state is prev
    assert events == []


def test_older_ts_ignored():
    """ts < last_ts -> игнор."""
    prev = make_unit("T-01", last_ts=ts(12, 0))
    msg = {"unit_uuid": "T-01", "ts": ts(11, 59), "lat": 0.0, "lon": 0.0, "speed_kmh": 0.0}
    state, events = apply_telemetry(prev, msg)
    assert state is prev


def test_late_but_newer_ts_accepted():
    """ts > last_ts, но ts < now (опоздавшее) -> учитывается."""
    prev = make_unit("T-01", last_ts=ts(12, 0))
    msg = {"unit_uuid": "T-01", "ts": ts(12, 0, 5), "lat": S1_POINT.lat + 0.02, "lon": S1_POINT.lon, "speed_kmh": 20.0}
    state, events = apply_telemetry(prev, msg)
    assert state is not prev
    assert state.last_ts == ts(12, 0, 5)


# --- 7. Чистота ---

def test_does_not_mutate_prev():
    """apply_telemetry не мутирует prev (чистая функция)."""
    prev = make_unit("T-01", last_ts=ts(11, 59), status=UnitStatus.TO_STATION)
    prev_status_before = prev.status
    prev_ts_before = prev.last_ts
    msg = {"unit_uuid": "T-01", "ts": ts(12, 0), "lat": S1_POINT.lat, "lon": S1_POINT.lon, "speed_kmh": 0.0}
    apply_telemetry(prev, msg)
    assert prev.status == prev_status_before
    assert prev.last_ts == prev_ts_before