from core.config import MIN_GAIN_SECONDS, FRESHNESS_SECONDS
from core.func import _wait_at_station, _closer
from core.models import UnitState, Decision, StationState, DecisionResult, RejectReason


def decide(
    unit: UnitState,
    stations: dict[str, StationState],
    now: int,
    event_ts: int,
) -> Decision:
    """Сравнить ожидание у всех станций, вернуть решение."""
    if now - event_ts > FRESHNESS_SECONDS:
        return Decision(
            unit_uuid=unit.unit_uuid,
            at=now,
            result=DecisionResult.REJECTED,
            reason=RejectReason.STALE_TELEMETRY,
        )
    home = unit.home_station
    home_wait = _wait_at_station(unit, stations[home], now)
    best_uuid, best_wait = home, home_wait

    for st_uuid, st in stations.items():
        if st_uuid == home:
            continue
        w = _wait_at_station(unit, st, now)
        if w < best_wait or (w == best_wait and _closer(unit, st_uuid, best_uuid)):
            best_uuid, best_wait = st_uuid, w

    gain = home_wait - best_wait
    if best_uuid == home or gain < MIN_GAIN_SECONDS:
        return Decision(unit.unit_uuid,
                        now, DecisionResult.REJECTED,
                        reason=RejectReason.NO_GAIN)

    return Decision(
        unit_uuid=unit.unit_uuid, at=now, result=DecisionResult.RECOMMENDED,
        from_station=home, to_station=best_uuid, gain_seconds=gain,
    )
