import argparse

from confluent_kafka.admin import AdminClient, NewTopic


TOPICS = {
    "telemetry.v1":       (8, 1, {}),
    "unit_events.v1":     (8, 1, {}),
    "station_events.v1":  (4, 1, {}),
    "queue.v1":           (4, 1, {}),
    "station_q.v1":       (4, 1, {"cleanup.policy": "compact"}),
    "decision.v1":        (8, 1, {}),
}


def create_topics(bootstrap: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})

    existing = set(admin.list_topics(timeout=5).topics.keys())

    to_create = []
    for name, (partitions, replication, config) in TOPICS.items():
        if name in existing:
            print(f"  skip: {name} (already exists)")
            continue
        to_create.append(NewTopic(
            name,
            num_partitions=partitions,
            replication_factor=replication,
            config=config,
        ))
        print(f"  create: {name} ({partitions} partitions, replication={replication})")

    if not to_create:
        print("Nothing to create.")
        return

    futures = admin.create_topics(to_create)
    for name, fut in futures.items():
        try:
            fut.result()
            print(f"  ok: {name}")
        except Exception as e:
            print(f"  error: {name}: {e}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    args = ap.parse_args()
    print(f"Creating topics on {args.bootstrap}...")
    create_topics(args.bootstrap)
    print("Done.")


if __name__ == "__main__":
    main()
