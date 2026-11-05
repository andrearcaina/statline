import json

import pyarrow.parquet as pq
from confluent_kafka import Producer

from .constants import (
    BATCH_SIZE,
    BOOTSTRAP_SERVERS,
    DATA_PATH,
    END_MESSAGE_TYPE,
    PARTITION,
    TOPIC,
)


def producer() -> None:
    table = pq.read_table(DATA_PATH).sort_by([("timeActual", "ascending")])
    kafka_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    sent = 0

    try:
        for batch in table.to_batches(max_chunksize=BATCH_SIZE):
            for row in batch.to_pylist():
                while True:
                    try:
                        kafka_producer.produce(
                            TOPIC,
                            value=json.dumps(row),
                            partition=PARTITION,
                        )
                        break
                    except BufferError:
                        kafka_producer.poll(0.1)

                kafka_producer.poll(0)
                sent += 1
                if sent % BATCH_SIZE == 0:
                    print(f"sent {sent}/{table.num_rows}")

        kafka_producer.produce(
            TOPIC,
            value=json.dumps({"type": END_MESSAGE_TYPE}),
            partition=PARTITION,
        )
    finally:
        kafka_producer.flush()

    print(f"finished: {sent} rows")


if __name__ == "__main__":
    producer()
