from core.config import GeoPoint, UNIT_HOME_STATION, DECISION_RADIUS_M, STATIONS_BY_UUID, STATION_RADIUS_M, \
    STANDING_SPEED_KMH, UNLOAD_POINT, UNLOAD_RADIUS_M
from core.geo import distance_m, within_radius
from core.models import UnitState, UnitEvent, UnitStatus, UnitEventType


# P1: применить одно сообщение телеметрии
def apply_telemetry(
    prev: UnitState | None,
    msg: dict,
) -> tuple[UnitState, list[UnitEvent]]:
    ts = int(msg["ts"])
    point = GeoPoint(msg["lat"], msg["lon"])
    speed = float(msg["speed_kmh"])
    unit_uuid = msg["unit_uuid"]

    # 1) порядок/дубли
    if prev is not None and ts <= prev.last_ts:
        return prev, []

    # новое состояние по умолчанию — как было (или дефолт для первой встречи)
    home = prev.home_station if prev else _home_station(msg["unit_uuid"])
    target = prev.target_station if prev else home
    decision_made = prev.decision_made_this_trip if prev else False
    last_decision_ts = prev.last_decision_ts if prev else None

    events: list[UnitEvent] = []

    # определить статус по правилам ТЗ
    station_point = STATIONS_BY_UUID[target].point
    near_station = within_radius(point, station_point, STATION_RADIUS_M)
    standing = speed < STANDING_SPEED_KMH
    entered_radius_ts = prev.entered_station_radius_ts if prev else None
    if near_station and standing:
        new_status = UnitStatus.AT_STATION
    elif prev is not None and prev.status == UnitStatus.AT_STATION and not near_station:
        new_status = UnitStatus.TO_UNLOAD
        entered_radius_ts = None
        events.append(UnitEvent(msg["unit_uuid"], ts, UnitEventType.LEFT_STATION, target))
    elif prev is not None and prev.status == UnitStatus.TO_UNLOAD and within_radius(point, UNLOAD_POINT, UNLOAD_RADIUS_M):
        # вошла в радиус разгрузки — пока ничего не эмитим, ждём выхода
        new_status = UnitStatus.TO_UNLOAD
    elif prev is not None and prev.status == UnitStatus.TO_UNLOAD and not within_radius(point, UNLOAD_POINT, UNLOAD_RADIUS_M):
        new_status = UnitStatus.TO_STATION
        target = home
        events.append(UnitEvent(msg["unit_uuid"], ts, UnitEventType.LEFT_UNLOAD, target))
    elif prev is None and not near_station:
        new_status = UnitStatus.TO_STATION
    else:
        new_status = prev.status if prev else UnitStatus.TO_STATION

    # вход в радиус станции
    if new_status == UnitStatus.AT_STATION and (prev is None or prev.status != UnitStatus.AT_STATION):
        entered_radius_ts = ts
        events.append(UnitEvent(msg["unit_uuid"], ts, UnitEventType.ARRIVED_AT_STATION, target))

    # пересечение радиуса решения (только в to_station и ещё не решали)
    if new_status == UnitStatus.TO_STATION and not decision_made:
        dist_home = distance_m(point, STATIONS_BY_UUID[home].point)
        if dist_home <= DECISION_RADIUS_M:
            events.append(UnitEvent(msg["unit_uuid"], ts, UnitEventType.CROSSED_DECISION_RADIUS, target))
            decision_made = True
            last_decision_ts = ts

    # приехала на станцию -> сброс «решено за заезд»
    if new_status == UnitStatus.AT_STATION:
        decision_made = False
        last_decision_ts = None

    new_state = UnitState(
        unit_uuid=unit_uuid, last_ts=ts, point=point, speed_kmh=speed,
        status=new_status, target_station=target, home_station=home,
        decision_made_this_trip=decision_made, last_decision_ts=last_decision_ts,
        entered_station_radius_ts=entered_radius_ts,
    )
    return new_state, events


def _home_station(unit_uuid: str) -> str:
    return UNIT_HOME_STATION[unit_uuid]