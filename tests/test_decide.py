"""Тесты P3: decide — рекомендация, отказ, tie-break, stale.

Правила ТЗ:
- stale_telemetry: now - event_ts > 30 -> rejected.
- ожидание у станции: free_at последней, кто приедет раньше нашей, минус eta, >= 0.
- выбор: min ожидание; при равенстве — своя, затем ближайшая.
- gain = home_wait - best_wait; gain < 60 -> no_gain.
- recommended только если best != home и gain >= 60.
"""
from __future__ import annotations

import pytest

from core.config import (
    FRESHNESS_SECONDS,
    MIN_GAIN_SECONDS,
    STATIONS_BY_UUID,
)
from core.models import (
    DecisionResult,
    QueueEntry,
    RejectReason,
    StationState,
    UnitStatus,
)
from core.decide import decide
from tests.conftest import make_unit, ts, point_at_distance_from

S1_POINT = STATIONS_BY_UUID["S1"].point
S2_POINT = STATIONS_BY_UUID["S2"].point


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------




def entry(uuid: str, eta: int | None, service_start: int, free_at: int, wait: int) -> QueueEntry:
    return QueueEntry(unit_uuid=uuid, eta=eta, service_start=service_start,
                      free_at=free_at, wait_seconds=wait)


def free_station(uuid: str) -> StationState:
    return StationState(uuid=uuid, occupied_by=None, occupied_until=0)


def occupied_station(uuid: str, by: str, since: int, until: int) -> StationState:
    return StationState(uuid=uuid, occupied_by=by, occupied_since=since,
                        occupied_until=until)


# ---------------------------------------------------------------------------
# 1. STALE_TELEMETRY
# ---------------------------------------------------------------------------

def test_stale_telemetry_when_event_older_than_30s():
    """now - event_ts > 30 -> rejected: stale_telemetry."""
    unit = make_unit("T-04", status=UnitStatus.TO_STATION)
    now = ts(12, 0, 0)
    event_ts = now - (FRESHNESS_SECONDS + 1)          # 31 с назад

    decision = decide(unit, stations={}, now=now, event_ts=event_ts)

    assert decision.result == DecisionResult.REJECTED
    assert decision.reason == RejectReason.STALE_TELEMETRY
    assert decision.unit_uuid == "T-04"


def test_not_stale_at_boundary_30s():
    """now - event_ts == 30 -> не stale (строгое >)."""
    unit = make_unit("T-04", status=UnitStatus.TO_STATION)
    now = ts(12, 0, 0)
    event_ts = now - FRESHNESS_SECONDS                # ровно 30

    s1 = free_station("S1")
    decision = decide(unit, stations={"S1": s1}, now=now, event_ts=event_ts)

    # не stale -> идёт обычный расчёт; своя свободна -> no_gain
    assert decision.result == DecisionResult.REJECTED
    assert decision.reason == RejectReason.NO_GAIN


def test_stale_telemetry_takes_priority_over_gain():
    """Даже если выигрыш был бы большой — stale важнее (проверка в начале)."""
    unit = make_unit("T-04", status=UnitStatus.TO_STATION)
    now = ts(12, 0, 0)
    event_ts = now - 60

    # S1 занята надолго, S2 свободна -> gain был бы большой
    s1 = occupied_station("S1", "T-01", ts(11, 59), ts(12, 10))
    s2 = free_station("S2")

    decision = decide(unit, stations={"S1": s1, "S2": s2}, now=now, event_ts=event_ts)
    assert decision.reason == RejectReason.STALE_TELEMETRY


# ---------------------------------------------------------------------------
# 2. Рекомендация (gain >= 60)
# ---------------------------------------------------------------------------

def test_example_from_spec_t04():
    """Пример из ТЗ: T-04, S1 (80 c), S2 (0 c) -> recommendation S2, gain 80."""
    now = ts(12, 0, 0)

    # T-04 в 900 м от S1 -> ETA 90 с -> 12:01:30
    point = point_at_distance_from(S1_POINT, 900)
    unit = make_unit("T-04", status=UnitStatus.TO_STATION,
                     home="S1", target="S1", point=point, last_ts=now)

    # S1 занята T-01 до 12:02:50 -> wait = 12:02:50 - 12:01:30 = 80
    s1 = occupied_station("S1", "T-01", ts(11, 59), ts(12, 2, 50))
    # S2 свободна -> wait = 0
    s2 = free_station("S2")

    decision = decide(unit, stations={"S1": s1, "S2": s2}, now=now, event_ts=now)

    assert decision.result == DecisionResult.RECOMMENDED
    assert decision.from_station == "S1"
    assert decision.to_station == "S2"
    assert decision.gain_seconds == 80
    assert decision.reason is None


def test_recommendation_when_gain_exactly_threshold():
    """gain == 60 -> recommended (порог включающий)."""
    now = ts(12, 0, 0)
    # T-04 в 900 м от S1 -> ETA 90 -> 12:01:30
    point = point_at_distance_from(S1_POINT, 900)
    unit = make_unit("T-04", status=UnitStatus.TO_STATION,
                     home="S1", target="S1", point=point, last_ts=now)

    # S1: wait 60 -> освободится в 12:02:30
    s1 = occupied_station("S1", "T-01", ts(11, 59), ts(12, 2, 30))
    # S2 свободна -> wait 0
    s2 = free_station("S2")

    decision = decide(unit, stations={"S1": s1, "S2": s2}, now=now, event_ts=now)
    assert decision.result == DecisionResult.RECOMMENDED
    assert decision.gain_seconds == MIN_GAIN_SECONDS


def test_recommendation_with_queue_not_only_occupied():
    """Рекомендация учитывает очередь: free_at последней, кто приедет раньше."""
    now = ts(12, 0, 0)
    point = point_at_distance_from(S1_POINT, 900)      # ETA 90 -> 12:01:30
    unit = make_unit("T-04", status=UnitStatus.TO_STATION,
                     home="S1", target="S1", point=point, last_ts=now)

    # S1 свободна, но в очереди T-02 (приедет раньше T-04) -> free_at 12:04:00
    s1 = StationState(
        uuid="S1", occupied_by=None, occupied_until=0,
        queue=[entry("T-02", eta=ts(12, 0, 30),
                     service_start=ts(12, 1, 0), free_at=ts(12, 4, 0), wait=30)],
    )
    s2 = free_station("S2")

    decision = decide(unit, stations={"S1": s1, "S2": s2}, now=now, event_ts=now)
    assert decision.result == DecisionResult.RECOMMENDED
    assert decision.to_station == "S2"
    # wait S1 = 12:04:00 - 12:01:30 = 150; wait S2 = 0 -> gain 150
    assert decision.gain_seconds == 150


# ---------------------------------------------------------------------------
# 3. Отказ no_gain
# ---------------------------------------------------------------------------

def test_no_gain_when_best_is_home():
    """Лучшая — своя (свободна, другие заняты) -> no_gain."""
    now = ts(12, 0, 0)
    point = point_at_distance_from(S1_POINT, 900)
    unit = make_unit("T-04", status=UnitStatus.TO_STATION,
                     home="S1", target="S1", point=point, last_ts=now)

    s1 = free_station("S1")
    # S2 занята надолго -> хуже
    s2 = occupied_station("S2", "T-05", ts(11, 59), ts(12, 10))

    decision = decide(unit, stations={"S1": s1, "S2": s2}, now=now, event_ts=now)
    assert decision.result == DecisionResult.REJECTED
    assert decision.reason == RejectReason.NO_GAIN


def test_no_gain_when_gain_below_threshold():
    """gain < 60 -> no_gain."""
    now = ts(12, 0, 0)
    point = point_at_distance_from(S1_POINT, 900)      # ETA 90 -> 12:01:30
    unit = make_unit("T-04", status=UnitStatus.TO_STATION,
                     home="S1", target="S1", point=point, last_ts=now)

    # S1: wait 20 -> освободится 12:01:50
    s1 = occupied_station("S1", "T-01", ts(11, 59), ts(12, 1, 50))
    # S2: свободна -> wait 0 (гарантированно)
    s2 = free_station("S2")

    decision = decide(unit, stations={"S1": s1, "S2": s2}, now=now, event_ts=now)
    assert decision.result == DecisionResult.REJECTED
    assert decision.reason == RejectReason.NO_GAIN


def test_no_gain_boundary_59():
    """gain == 59 -> no_gain (строгое < 60)."""
    now = ts(12, 0, 0)
    point = point_at_distance_from(S1_POINT, 900)      # ETA 90 -> 12:01:30
    unit = make_unit("T-04", status=UnitStatus.TO_STATION,
                     home="S1", target="S1", point=point, last_ts=now)

    # S1: wait 59 -> 12:02:29
    s1 = occupied_station("S1", "T-01", ts(11, 59), ts(12, 2, 29))
    s2 = free_station("S2")

    decision = decide(unit, stations={"S1": s1, "S2": s2}, now=now, event_ts=now)
    assert decision.result == DecisionResult.REJECTED
    assert decision.reason == RejectReason.NO_GAIN


# ---------------------------------------------------------------------------
# 4. Tie-break
# ---------------------------------------------------------------------------

def test_home_wins_on_tie_with_free_stations():
    """Все станции свободны -> ожидание 0 везде -> своя выигрывает -> no_gain."""
    now = ts(12, 0, 0)
    unit = make_unit("T-04", status=UnitStatus.TO_STATION,
                     home="S1", target="S1", last_ts=now)
    stations = {u: free_station(u) for u in ("S1", "S2", "S3", "S4")}

    decision = decide(unit, stations=stations, now=now, event_ts=now)
    assert decision.result == DecisionResult.REJECTED
    assert decision.reason == RejectReason.NO_GAIN


def test_closer_wins_on_tie_between_others():
    """S2 и S3 с одинаковым ожиданием -> выбирается ближайшая."""
    now = ts(12, 0, 0)
    # T-04 ближе к S2, чем к S3
    point = point_at_distance_from(S2_POINT, 500)
    unit = make_unit("T-04", status=UnitStatus.TO_STATION,
                     home="S1", target="S1", point=point, last_ts=now)

    # S1 занята надолго -> худшая
    s1 = occupied_station("S1", "T-01", ts(11, 59), ts(12, 10))
    # S2 и S3 свободны -> ожидание 0 у обеих -> tie-break по близости
    s2 = free_station("S2")
    s3 = free_station("S3")

    decision = decide(unit, stations={"S1": s1, "S2": s2, "S3": s3},
                      now=now, event_ts=now)
    assert decision.result == DecisionResult.RECOMMENDED
    assert decision.to_station == "S2"           # ближе, чем S3


# ---------------------------------------------------------------------------
# 5. Decision: поля
# ---------------------------------------------------------------------------

def test_recommended_decision_fields():
    """recommended: заполнены from/to/gain, reason=None."""
    now = ts(12, 0, 0)
    point = point_at_distance_from(S1_POINT, 900)
    unit = make_unit("T-04", status=UnitStatus.TO_STATION,
                     home="S1", target="S1", point=point, last_ts=now)

    s1 = occupied_station("S1", "T-01", ts(11, 59), ts(12, 2, 50))
    s2 = free_station("S2")

    d = decide(unit, stations={"S1": s1, "S2": s2}, now=now, event_ts=now)

    assert d.unit_uuid == "T-04"
    assert d.at == now
    assert d.result == DecisionResult.RECOMMENDED
    assert d.from_station == "S1"
    assert d.to_station == "S2"
    assert d.gain_seconds is not None and d.gain_seconds >= MIN_GAIN_SECONDS
    assert d.reason is None


def test_rejected_decision_fields_no_gain():
    """rejected no_gain: from/to/gain=None, reason=NO_GAIN."""
    now = ts(12, 0, 0)
    unit = make_unit("T-04", status=UnitStatus.TO_STATION,
                     home="S1", target="S1", last_ts=now)
    stations = {u: free_station(u) for u in ("S1", "S2")}

    d = decide(unit, stations=stations, now=now, event_ts=now)

    assert d.result == DecisionResult.REJECTED
    assert d.reason == RejectReason.NO_GAIN
    assert d.from_station is None
    assert d.to_station is None
    assert d.gain_seconds is None


def test_rejected_decision_fields_stale():
    """rejected stale: reason=STALE_TELEMETRY, from/to/gain=None."""
    now = ts(12, 0, 0)
    unit = make_unit("T-04", status=UnitStatus.TO_STATION)

    d = decide(unit, stations={}, now=now, event_ts=now - 60)

    assert d.result == DecisionResult.REJECTED
    assert d.reason == RejectReason.STALE_TELEMETRY
    assert d.from_station is None
    assert d.to_station is None
    assert d.gain_seconds is None


def test_decision_unit_uuid_is_real_not_empty():
    """Decision.unit_uuid — реальный id (не пустая строка)."""
    now = ts(12, 0, 0)
    unit = make_unit("T-42", status=UnitStatus.TO_STATION)

    d = decide(unit, stations={u: free_station(u) for u in ("S1", "S2")},
               now=now, event_ts=now)
    assert d.unit_uuid == "T-42"
    assert d.unit_uuid != ""


# ---------------------------------------------------------------------------
# 6. Несколько станций: выбирается минимум
# ---------------------------------------------------------------------------

def test_picks_minimum_across_all_stations():
    """Из 4 станций выбирается с наименьшим ожиданием."""
    now = ts(12, 0, 0)
    point = point_at_distance_from(S1_POINT, 900)      # ETA 90 -> 12:01:30
    unit = make_unit("T-04", status=UnitStatus.TO_STATION,
                     home="S1", target="S1", point=point, last_ts=now)

    s1 = occupied_station("S1", "T-01", ts(11, 59), ts(12, 2, 50))   # wait 80
    s2 = occupied_station("S2", "T-02", ts(11, 59), ts(12, 5, 0))    # хуже
    s3 = free_station("S3")                                           # wait 0
    s4 = occupied_station("S4", "T-03", ts(11, 59), ts(12, 5, 0))   # wait 20

    d = decide(unit, stations={"S1": s1, "S2": s2, "S3": s3, "S4": s4},
               now=now, event_ts=now)
    assert d.result == DecisionResult.RECOMMENDED
    assert d.to_station == "S3"           # минимум (0)
    assert abs(d.gain_seconds - 80) <= 2
