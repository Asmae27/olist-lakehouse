"""Règle « date malformée » du flux : distinguer absent (légitime) de illisible (à rejeter)."""

from pyspark.sql import functions as F

from batch.silver_quarantine import split_valid_and_quarantine
from streaming.silver_orders_from_stream import DATE_COLS, malformed_rule


def _typed(spark, raw_value):
    """Reproduit l'étape de typage du job : on garde _raw_ puis on convertit."""
    df = spark.createDataFrame(
        [("o1", "2018-01-01 10:00:00", raw_value)],
        "order_id string, order_purchase_timestamp string, order_approved_at string",
    )
    for c in ("order_purchase_timestamp", "order_approved_at"):
        df = df.withColumn(f"_raw_{c}", F.col(c))
        df = df.withColumn(c, F.to_timestamp(F.when(F.trim(F.col(c)) != "", F.col(c))))
    return df


def _split(df):
    rules = [malformed_rule(c) for c in ("order_purchase_timestamp", "order_approved_at")]
    return split_valid_and_quarantine(df, "orders", extra_rules=rules)


def test_empty_string_date_is_legitimately_absent(spark):
    valid, quarantine = _split(_typed(spark, ""))
    assert valid.count() == 1 and quarantine.count() == 0


def test_null_date_is_legitimately_absent(spark):
    valid, quarantine = _split(_typed(spark, None))
    assert valid.count() == 1 and quarantine.count() == 0


def test_unparseable_date_is_quarantined(spark):
    valid, quarantine = _split(_typed(spark, "pas-une-date"))
    assert valid.count() == 0
    assert quarantine.collect()[0]["quarantine_reason"] == ["order_approved_at_malformed"]


def test_one_malformed_rule_per_date_column():
    assert len({malformed_rule(c)[0] for c in DATE_COLS}) == len(DATE_COLS)


def test_valid_date_is_not_quarantined(spark):
    valid, quarantine = _split(_typed(spark, "2018-01-02 14:30:00"))

    assert valid.count() == 1
    assert quarantine.count() == 0


def test_whitespace_date_is_legitimately_absent(spark):
    valid, quarantine = _split(_typed(spark, "   "))

    assert valid.count() == 1
    assert quarantine.count() == 0


def test_malformed_rule_has_expected_name():
    for col_name in DATE_COLS:
        rule_name, condition_fn = malformed_rule(col_name)

        assert rule_name == f"{col_name}_malformed"
        assert callable(condition_fn)
