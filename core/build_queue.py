from core.config import QUEUE_HORIZON_SECONDS, OCCUPANCY_SECONDS, FRESHNESS_SECONDS
from core.func import _eta_to_station
from core.models import StationState, UnitState, QueueEntry, UnitStatus, UnitEventType


# P2: построить очередь станции
def build_queue(
    station: StationState,
    units: dict[str, UnitState],
    now: int,
) -> list[QueueEntry]:
    """Пересчитать очередь станции на горизонт 30 минут от now.

    units — состояния ВСЕХ машин (P2 хранит копию только релевантных,
    но для чистоты принимает все; фильтрует сам).
    """
    horizon = now + QUEUE_HORIZON_SECONDS
    entries: list[QueueEntry] = []

    # 1) занявшая станция — первая
    if station.occupied_by is not None:
        service_start = station.occupied_since
        if service_start is None:
            service_start = station.occupied_until - OCCUPANCY_SECONDS
        entries.append(QueueEntry(
            unit_uuid=station.occupied_by,
            eta=None,
            service_start=service_start,
            free_at=station.occupied_until,
            wait_seconds=0,
        ))

    # 2) кандидаты: едут к этой станции, свежие, не занявшая
    candidates: list[tuple[int, str]] = []   # (eta_ts, unit_uuid)
    for uuid, st in units.items():
        if uuid == station.occupied_by:
            continue
        if st.target_station != station.uuid:
            continue
        if now - st.last_ts > FRESHNESS_SECONDS:
            continue  # stale
        if st.status == UnitStatus.AT_STATION:
            if st.entered_station_radius_ts is None:
                continue
            candidates.append((st.entered_station_radius_ts, uuid))
            continue

        eta = _eta_to_station(st, station.uuid, now)
        if eta is None or eta > horizon:
            continue
        candidates.append((eta, uuid))

    candidates.sort()

    # 3) выстраиваем по времени приезда
    prev_free = station.occupied_until if station.occupied_by else now
    for eta, uuid in candidates:
        service_start = max(eta, prev_free)
        free_at = service_start + OCCUPANCY_SECONDS
        entries.append(QueueEntry(
            unit_uuid=uuid, eta=eta,
            service_start=service_start, free_at=free_at,
            wait_seconds=service_start - eta,
        ))
        prev_free = free_at
    return entries


def handle_event(
    station: StationState,
    event: dict,
    units: dict[str, UnitState],
    now: int,
) -> StationState:
    """Применить одно событие к состоянию станции. Вернуть новое состояние."""
    etype = event["type"]
    unit_uuid = event["unit_uuid"]
    ts = int(event["ts"])

    if etype == UnitEventType.ARRIVED_AT_STATION.value:
        if station.occupied_by is None:
            # станция свободна — занять
            station.occupied_by = unit_uuid
            station.occupied_since = ts                          # ← ФАКТ занятия
            station.occupied_until = ts + OCCUPANCY_SECONDS      # ← расчётное освобождение
        # иначе — ждущая, очередь пересчитается в build_queue

    elif etype == UnitEventType.LEFT_STATION.value:
        if unit_uuid == station.occupied_by:
            # уехала занявшая — ФАКТ выезда
            station.occupied_until = ts                          # ← факт (не min!)
            station.occupied_by = None
            station.occupied_since = None                        # ← сброс
        # если уехала ждущая — станция не освобождается,
        # но entered_station_radius_ts уже сброшен в P1 → из очереди уйдёт

    # остальные события (LEFT_UNLOAD, EN_ROUTE_TO_STATION) —
    # влияют на очередь через units, не на occupied_*
    return station
