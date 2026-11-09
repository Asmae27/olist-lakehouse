from unittest.mock import MagicMock

import pytest

from batch import silver_quarantine


def create_orders(spark):
    """Crée un DataFrame avec une ligne valide et une ligne invalide."""
    return spark.createDataFrame(
        [
            ("order-1", "2018-01-01"),
            (None, "2018-01-02"),
        ],
        ["order_id", "order_purchase_timestamp"],
    )


def test_process_table_writes_valid_and_quarantined_rows(spark, monkeypatch):
    df = create_orders(spark)
    written_tables = []

    monkeypatch.setattr(
        silver_quarantine,
        "read_table",
        lambda spark, layer, table: df,
    )

    monkeypatch.setattr(
        silver_quarantine,
        "write_table",
        lambda data, layer, table: written_tables.append((data, layer, table)),
    )

    silver_quarantine.process_table(spark, "orders")

    assert len(written_tables) == 2

    valid_df, valid_layer, valid_table = written_tables[0]
    quarantine_df, quarantine_layer, quarantine_table = written_tables[1]

    assert valid_layer == "silver"
    assert quarantine_layer == "quarantine"
    assert valid_table == "orders"
    assert quarantine_table == "orders"

    assert valid_df.count() == 1
    assert quarantine_df.count() == 1

    assert quarantine_df.collect()[0]["quarantine_reason"] == ["order_id_not_null"]


def test_process_table_warns_when_quarantine_rate_exceeds_threshold(spark, monkeypatch, capsys):
    df = create_orders(spark)

    monkeypatch.setattr(
        silver_quarantine,
        "read_table",
        lambda spark, layer, table: df,
    )

    monkeypatch.setattr(
        silver_quarantine,
        "write_table",
        lambda *args: None,
    )

    silver_quarantine.process_table(spark, "orders")

    captured = capsys.readouterr()

    assert "ALERTE QUALITÉ" in captured.out
    assert "50.0%" in captured.out


def test_process_table_does_not_warn_when_table_is_empty(spark, monkeypatch, capsys):
    df = spark.createDataFrame(
        [],
        "order_id STRING, order_purchase_timestamp STRING",
    )

    written_tables = []

    monkeypatch.setattr(
        silver_quarantine,
        "read_table",
        lambda spark, layer, table: df,
    )

    monkeypatch.setattr(
        silver_quarantine,
        "write_table",
        lambda data, layer, table: written_tables.append((data, layer, table)),
    )

    silver_quarantine.process_table(spark, "orders")

    captured = capsys.readouterr()

    assert len(written_tables) == 2
    assert "ALERTE QUALITÉ" not in captured.out

    for data, _, _ in written_tables:
        assert data.count() == 0


def test_process_table_raises_when_reconciliation_fails(spark, monkeypatch):
    df = create_orders(spark)

    valid_df = df.limit(1)
    quarantine_df = df.limit(0)

    monkeypatch.setattr(
        silver_quarantine,
        "read_table",
        lambda spark, layer, table: df,
    )

    monkeypatch.setattr(
        silver_quarantine,
        "split_valid_and_quarantine",
        lambda df, table_name: (valid_df, quarantine_df),
    )

    write_mock = MagicMock()

    monkeypatch.setattr(
        silver_quarantine,
        "write_table",
        write_mock,
    )

    with pytest.raises(RuntimeError, match="Réconciliation échouée"):
        silver_quarantine.process_table(spark, "orders")

    # Aucune donnée ne doit être publiée si la réconciliation échoue.
    write_mock.assert_not_called()


def test_main_initializes_and_processes_all_tables(monkeypatch):
    fake_spark = MagicMock()

    ensure_layers_mock = MagicMock()
    get_spark_mock = MagicMock(return_value=fake_spark)
    process_table_mock = MagicMock()
    stop_spark_mock = MagicMock()

    monkeypatch.setattr(silver_quarantine, "ensure_layers", ensure_layers_mock)
    monkeypatch.setattr(silver_quarantine, "get_spark", get_spark_mock)
    monkeypatch.setattr(silver_quarantine, "process_table", process_table_mock)
    monkeypatch.setattr(silver_quarantine, "stop_spark", stop_spark_mock)

    silver_quarantine.main()

    ensure_layers_mock.assert_called_once_with(["silver", "quarantine"])

    get_spark_mock.assert_called_once_with("silver-quarantine")

    expected_tables = list(silver_quarantine.DATA_QUALITY_RULES)

    assert process_table_mock.call_count == len(expected_tables)

    processed_tables = [call.args[1] for call in process_table_mock.call_args_list]

    assert processed_tables == expected_tables

    stop_spark_mock.assert_called_once_with(fake_spark)
