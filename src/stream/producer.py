import json

import pyarrow.parquet as pq
from confluent_kafka import Producer

from .constants import (
    BATCH_SIZE,
    BOOTSTRAP_SERVERS,
    DATA_PATH,
    END_MESSAGE_TYPE,
    PARTITION,
    PRINT_EVERY,
    TOPIC,
)


def producer() -> None:
    table = pq.read_table(DATA_PATH).sort_by([("timeActual", "ascending")])
    kafka_producer = Producer(
        {"bootstrap.servers": BOOTSTRAP_SERVERS, "compression.type": "lz4", "message.timeout.ms": 60_000}
    )
    sent = 0
    delivery_errors = []

    def delivered(error, _message):
        if error is not None and not delivery_errors:
            delivery_errors.append(str(error))

    def send(row):
        while True:
            if delivery_errors:
                raise RuntimeError(f"Kafka delivery failed: {delivery_errors[0]}")
            try:
                kafka_producer.produce(
                    TOPIC,
                    value=json.dumps(row),
                    partition=PARTITION,
                    on_delivery=delivered,
                )
                break
            except BufferError:
                kafka_producer.poll(0.1)
        kafka_producer.poll(0)

    try:
        for batch in table.to_batches(max_chunksize=BATCH_SIZE):
            for row in batch.to_pylist():
                send(row)
                sent += 1
                if sent % PRINT_EVERY == 0:
                    print(f"sent {sent}/{table.num_rows}", flush=True)

        send({"type": END_MESSAGE_TYPE})
    finally:
        remaining = kafka_producer.flush(65)

    if remaining or delivery_errors:
        raise RuntimeError(f"Kafka delivery failed: {delivery_errors or f'{remaining} messages still queued'}")

    print(f"finished: {sent} rows")


if __name__ == "__main__":
    producer()
