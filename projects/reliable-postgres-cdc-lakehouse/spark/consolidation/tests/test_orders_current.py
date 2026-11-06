import random
from pyspark.sql import SparkSession
from orders_cdc_parser import build_orders_current

SCHEMA = """
    id_order string, id_client string, status_order string, amount decimal(10,2),
    created_at timestamp, updated_at timestamp, cdc_operation string,
    source_lsn long, source_tx_id long, source_sequence string,
    source_ts_ms long, cdc_ts_ms long, topic string,
    kafka_partition int, `offset` long, kafka_timestamp_utc timestamp
"""


def make_event(fields, id_order, lsn, offset, status):
    row = dict.fromkeys(fields)
    row.update(id_order=id_order, source_lsn=lsn, offset=offset, status_order=status)
    return row


def run_current(spark, events, partitions=None):
    df = spark.createDataFrame(events, SCHEMA)
    if partitions:
        df = df.repartition(partitions)
    rows = build_orders_current(df).collect()
    return {r["id_order"]: r["status_order"] for r in rows}


def main():
    spark = SparkSession.builder.master("local[4]").appName("test-orders-current").getOrCreate()
    fields = spark.createDataFrame([], SCHEMA).columns

    events = [
        make_event(fields, "A", 100, 9, "OLD"),     # Cenário 1: maior offset, mas menor LSN
        make_event(fields, "A", 200, 1, "NEW"),
        make_event(fields, "B", 300, 5, "FIRST"),   # Cenário 2: LSN empatado
        make_event(fields, "B", 300, 7, "SECOND"),
    ]

    try:
        # Cenário 1: o LSN manda; o offset só desempata
        result = run_current(spark, events)
        assert result["A"] == "NEW", f"Cenário 1 falhou: A -> {result['A']} (esperado NEW)"
        print(f"[OK] Cenário 1: A -> {result['A']}")

        # Cenário 2: com LSN igual, vence o maior offset
        assert result["B"] == "SECOND", f"Cenário 2 falhou: B -> {result['B']} (esperado SECOND)"
        print(f"[OK] Cenário 2: B -> {result['B']}")

        # Cenário 3: determinismo com a entrada embaralhada e reparticionada
        rng = random.Random(42)
        outcomes = set()
        for i in range(20):
            shuffled = events[:]
            rng.shuffle(shuffled)
            outcomes.add(run_current(spark, shuffled, partitions=(i % 8) + 1)["B"])
        assert outcomes == {"SECOND"}, f"Cenário 3 falhou: resultados observados {outcomes}"
        print(f"[OK] Cenário 3: 20 execuções -> {outcomes}")

        print("Todos os cenários passaram.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()