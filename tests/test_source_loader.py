"""Chargement de la base source PostgreSQL : SQL généré et portes de contrôle (sans base réelle)."""

from unittest.mock import MagicMock

import pytest

from common.config import get_source_db_settings
from source import load_source_db as loader


def make_csv(tmp_path, rows):
    path = tmp_path / "orders.csv"
    path.write_text("order_id,customer_id\n" + "".join(f"{a},{b}\n" for a, b in rows), encoding="utf-8")
    return path


SPEC = {
    "csv": "orders.csv",
    "primary_key": ["order_id"],
    "columns": [("order_id", "TEXT"), ("customer_id", "TEXT")],
}


def test_create_table_sql_declares_columns_and_composite_primary_key():
    sql = loader.create_table_sql(
        "order_items", [("order_id", "TEXT"), ("order_item_id", "INTEGER")], ["order_id", "order_item_id"]
    )
    assert "CREATE TABLE IF NOT EXISTS order_items" in sql
    assert "order_item_id INTEGER" in sql
    assert "PRIMARY KEY (order_id, order_item_id)" in sql


def test_copy_sql_lists_columns_in_order_and_skips_header():
    sql = loader.copy_sql("orders", [("order_id", "TEXT"), ("customer_id", "TEXT")])
    assert sql == "COPY orders (order_id, customer_id) FROM STDIN WITH (FORMAT csv, HEADER true)"


@pytest.mark.parametrize("name", list(loader.TABLES))
def test_every_table_has_a_primary_key_made_of_its_own_columns(name):
    spec = loader.TABLES[name]
    column_names = [col for col, _ in spec["columns"]]
    assert spec["primary_key"]
    assert set(spec["primary_key"]) <= set(column_names)


def test_count_csv_records_ignores_header_and_handles_quoted_newlines(tmp_path):
    path = tmp_path / "reviews.csv"
    path.write_text('id,comment\n1,"line one\nline two"\n2,ok\n', encoding="utf-8")
    assert loader.count_csv_records(str(path)) == 2  # `wc -l` dirait 4


def test_count_csv_records_ignores_utf8_bom(tmp_path):
    path = tmp_path / "bom.csv"
    path.write_bytes(b"\xef\xbb\xbforder_id\nA\nB\n")
    assert loader.count_csv_records(str(path)) == 2


def test_load_table_skips_a_table_that_is_already_filled(tmp_path):
    make_csv(tmp_path, [("o1", "c1")])
    cur = MagicMock()
    cur.fetchone.return_value = (42,)

    assert loader.load_table(cur, "orders", SPEC, str(tmp_path)) == 42

    cur.copy_expert.assert_not_called()
    cur.execute.assert_any_call("ALTER TABLE orders REPLICA IDENTITY FULL")


def test_load_table_copies_the_csv_into_an_empty_table(tmp_path):
    make_csv(tmp_path, [("o1", "c1"), ("o2", "c2")])
    cur = MagicMock()
    cur.fetchone.side_effect = [(0,), (2,)]  # vide avant, 2 lignes après

    assert loader.load_table(cur, "orders", SPEC, str(tmp_path)) == 2

    cur.copy_expert.assert_called_once()
    assert cur.copy_expert.call_args.args[0] == loader.copy_sql("orders", SPEC["columns"])


def test_load_table_fails_when_rows_are_lost(tmp_path):
    make_csv(tmp_path, [("o1", "c1"), ("o2", "c2")])
    cur = MagicMock()
    cur.fetchone.side_effect = [(0,), (1,)]  # 1 ligne chargée sur 2 attendues

    with pytest.raises(RuntimeError, match="1 lignes chargées, 2 attendues"):
        loader.load_table(cur, "orders", SPEC, str(tmp_path))


def test_reset_truncates_before_reloading(tmp_path):
    make_csv(tmp_path, [("o1", "c1")])
    cur = MagicMock()
    cur.fetchone.side_effect = [(5,), (1,)]

    loader.load_table(cur, "orders", SPEC, str(tmp_path), reset=True)

    cur.execute.assert_any_call("TRUNCATE orders")


def test_source_db_settings_default_to_the_compose_service(monkeypatch):
    monkeypatch.setenv("SOURCE_DB_USER", "u")
    monkeypatch.setenv("SOURCE_DB_PASSWORD", "p")
    for var in ("SOURCE_DB_HOST", "SOURCE_DB_PORT", "SOURCE_DB_NAME"):
        monkeypatch.delenv(var, raising=False)

    assert get_source_db_settings() == {
        "host": "source-db",
        "port": 5432,
        "dbname": "olist_source",
        "user": "u",
        "password": "p",
    }


def test_source_db_settings_refuse_to_start_without_credentials(monkeypatch):
    monkeypatch.delenv("SOURCE_DB_USER", raising=False)
    monkeypatch.setenv("SOURCE_DB_PASSWORD", "p")
    with pytest.raises(RuntimeError, match="SOURCE_DB_USER"):
        get_source_db_settings()
