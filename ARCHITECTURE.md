# Архитектура

Сервис строит виртуальные очереди к станциям из телеметрии и рекомендует машинам станции. Реализовано как **три независимых стриминговых процессора** на Kafka (Python + `confluent-kafka`).

## 1. Сколько процессоров и критерий

**Три процессора** (плюс генератор, не процессор).

**Критерий:** «каждый процессор владеет состоянием, которое естественно шардируется по одному ключу». Три разных ключа → три процессора:
- **P1 (Telemetry)** — состояние **per-unit** (ключ `unit_uuid`): нормализация телеметрии, детекция переходов.
- **P2 (Queue)** — состояние **per-station** (ключ `station_uuid`): занятость станции, очередь.
- **P3 (Recommender)** — решение **per-unit**, но требует **всех** станций (broadcast): выбор станции.

Разделение позволяет масштабировать каждый процессор независимо (по числу партиций своего ключа).

## 2. Ключ каждого процессора и что в состоянии

| Процессор | Ключ | Партиции | Состояние |
|---|---|---|---|
| **P1** | `unit_uuid` | 8 | `UnitState`: `last_ts`, `point`, `speed_kmh`, `status` (`AT_STATION`/`TO_UNLOAD`/`TO_STATION`), `target_station`, `home_station`, `decision_made_this_trip`, `entered_station_radius_ts` |
| **P2** | `station_uuid` | 4 | `StationState`: `occupied_by`, `occupied_since`, `occupied_until`, `queue` (список `QueueEntry`), `last_published_hash` |
| **P3** | `unit_uuid` | 8 | `units`: `dict[unit_uuid → UnitState]`; `stations`: `dict[station_uuid → StationState]` (из broadcast-снимков) |

**State store:** in-memory `dict` + компактированный changelog-топик (`state.<processor>.changelog`) для восстановления. Для 40 машин восстановление — секунды. В проде — RocksDB + changelog (как в Kafka Streams).

## 3. Конфликт «машина vs станция» — как разрешён

Телеметрия приходит **по машинам** (`unit_uuid`), очередь нужна **по станциям** (`station_uuid`), решение — **per-unit**, но требует **всех** станций.

**Решение — внутренние топики с перекешированием:**
1. **P1** эмитит каждое событие в **два** топика:
   - `unit_events.v1` (ключ `unit_uuid`) → для P3;
   - `station_events.v1` (ключ `station_uuid`) → для P2.
2. **P2** публикует снимок очереди в `station_q.v1` (ключ `station_uuid`, **compacted**).
3. **P3** читает:
   - свои `unit_events.v1` (по партициям своего ключа);
   - **broadcast** `station_q.v1` (**все** партиции, **уникальный** `group.id` на инстанс) — глобальная картина всех станций.
4. **P3** также читает `telemetry.v1` (своя группа `p3-units`) для `UnitState` (нужен для `decide`: позиция, `status`, `home`).

**Broadcast** — ключевой приём: топик станций **маленький** (4 записи), его можно реплицировать во все инстансы P3.

## 4. Партиции, реплики, параллелизм

| Топик | Партиции | Реплики (прод / локально) | Ограничение параллелизма |
|---|---|---|---|
| `telemetry.v1` | 8 | 3 / 1 | ≤ 8 инстансов P1 |
| `unit_events.v1` | 8 | 3 / 1 | ≤ 8 инстансов P3 |
| `station_events.v1` | 4 | 3 / 1 | ≤ 4 инстансов P2 |
| `queue.v1` | 4 | 3 / 1 | — |
| `station_q.v1` | 4 | 3 / 1 (compacted) | broadcast → не ограничивает P3 |
| `decision.v1` | 8 | 3 / 1 | — |

- **P1:** ≤ 8 инстансов (по партициям `telemetry.v1`).
- **P2:** ≤ 4 инстансов (по партициям `station_events.v1` / `station_uuid`).
- **P3:** ≤ 8 инстансов (по партициям `unit_events.v1`); broadcast `station_q.v1` **не** ограничивает (маленький, все читают всё).

Реплики: 3 для прода (`min.insync.replicas=2`), 1 — для локального docker-compose.

## 5. Гарантии доставки

| Топик | Гарантия | Как |
|---|---|---|
| `telemetry.v1` | at-least-once | consumer manual commit (после обработки) |
| `unit_events`/`station_events` | at-least-once + идемпотентность | правило `ts <= last_ts` отсекает дубли; state обновляется детерминированно |
| `queue.v1` | идемпотентность достаточно | дедуп по `last_published_hash`; повторная публикация того же снимка безвредна |
| `decision.v1` | **exactly-once желательно** | Kafka transactions (state.put + producer.send + offset.commit); иначе идемпотентность по `(unit_uuid, trip_id)` |

**Где транзакции обязательны:** P3 (`decision.v1` — решение не должно дублироваться за заезд).
**Где достаточно идемпотентности:** P2 (`queue.v1` — снимок, повтор безвреден).
