def main() -> None:
    # Technically, each folder has its own entrypoint, this is only a way to run the
    # complete pipeline at once for testing

    # batch: cdnnba.parquet -> PySpark -> partitioned/unpartitioned Parquet -> ClickHouse
    #
    # Stream: cdnnba.parquet -> producer -> Kafka -> consumer -> ClickHouse
    #
    # part 3 and 4 will be done later

    print("Hello World")


if __name__ == "__main__":
    main()
