from .stream.consumer import consumer
from .stream.producer import producer


def main() -> None:
    # Technically, each folder has its own entrypoint, this is only a way to run the
    # complete pipeline at once for testing

    # batch: cdnnba.parquet -> PySpark -> partitioned/unpartitioned Parquet -> ClickHouse
    #
    # Stream: unpartitioned/partitioned Parquet -> producer -> Kafka -> consumer -> ClickHouse
    #
    # part 3 and 4 will be done later

    # part 2
    # currently has a default DATA_PATH, will be partitioned parquet (or both)
    # later on after part 1 is complete
    producer()
    consumer()


if __name__ == "__main__":
    main()
