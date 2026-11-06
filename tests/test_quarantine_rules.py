"""Règles qualité Silver : la logique qui décide « valide » ou « quarantaine »."""

from batch.silver_quarantine import DATA_QUALITY_RULES, split_valid_and_quarantine


def _orders(spark, rows):
    return spark.createDataFrame(rows, "order_id string, order_purchase_timestamp string")


def test_valid_rows_go_to_silver_without_helper_columns(spark):
    df = _orders(spark, [("o1", "2018-01-01 10:00:00")])
    valid, quarantine = split_valid_and_quarantine(df, "orders")
    assert valid.count() == 1
    assert quarantine.count() == 0
    assert set(valid.columns) == {"order_id", "order_purchase_timestamp"}


def test_null_order_id_is_quarantined_with_its_reason(spark):
    df = _orders(spark, [(None, "2018-01-01 10:00:00")])
    valid, quarantine = split_valid_and_quarantine(df, "orders")
    assert valid.count() == 0
    reasons = quarantine.collect()[0]["quarantine_reason"]
    assert reasons == ["order_id_not_null"]


def test_row_failing_two_rules_lists_both_reasons(spark):
    df = _orders(spark, [(None, None)])
    _, quarantine = split_valid_and_quarantine(df, "orders")
    reasons = quarantine.collect()[0]["quarantine_reason"]
    assert sorted(reasons) == ["order_id_not_null", "purchase_timestamp_not_null"]


def test_reconciliation_no_row_lost_or_duplicated(spark):
    """Invariant de la porte bloquante : valid + quarantine == total."""
    df = _orders(
        spark,
        [
            ("o1", "2018-01-01"),
            (None, "2018-01-02"),
            ("o3", None),
            (None, None),
            ("o5", "2018-01-05"),
        ],
    )
    valid, quarantine = split_valid_and_quarantine(df, "orders")
    assert valid.count() + quarantine.count() == df.count() == 5
    assert valid.count() == 2


def test_extra_rules_are_applied_on_top_of_table_rules(spark):
    from pyspark.sql import functions as F

    df = _orders(spark, [("bad", "2018-01-01"), ("ok", "2018-01-01")])
    extra = [("id_not_bad", lambda: F.col("order_id") != "bad")]
    valid, quarantine = split_valid_and_quarantine(df, "orders", extra_rules=extra)
    assert [r.order_id for r in valid.collect()] == ["ok"]
    assert quarantine.collect()[0]["quarantine_reason"] == ["id_not_bad"]


def test_every_rule_name_is_unique_per_table():
    for table, rules in DATA_QUALITY_RULES.items():
        names = [name for name, _ in rules]
        assert len(names) == len(set(names)), f"règle dupliquée dans {table}"
