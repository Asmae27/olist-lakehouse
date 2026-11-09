"""Couche d'accès au stockage : le même code métier doit viser le bon endroit selon le backend."""

from unittest.mock import MagicMock

import pytest

from common import lake


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for var in ("LAKE_BACKEND", "LAKE_CATALOG", "LAKE_RAW_DIR"):
        monkeypatch.delenv(var, raising=False)


def test_default_backend_is_local():
    assert lake.get_backend() == "local"


def test_unknown_backend_is_rejected(monkeypatch):
    monkeypatch.setenv("LAKE_BACKEND", "snowflake")
    with pytest.raises(ValueError, match="LAKE_BACKEND"):
        lake.get_backend()


def test_local_table_ref_is_an_s3_path():
    assert lake.table_ref("silver", "orders") == "s3a://silver/orders/"


def test_databricks_table_ref_is_a_three_level_name(monkeypatch):
    monkeypatch.setenv("LAKE_BACKEND", "databricks")
    assert lake.table_ref("silver", "orders") == "workspace.silver.orders"


def test_catalog_can_be_overridden(monkeypatch):
    monkeypatch.setenv("LAKE_BACKEND", "databricks")
    monkeypatch.setenv("LAKE_CATALOG", "mon_catalogue")
    assert lake.table_ref("gold", "dim_date") == "mon_catalogue.gold.dim_date"


def test_raw_dir_per_backend(monkeypatch):
    assert lake.raw_dir() == "/opt/spark/work-dir/data/raw"
    monkeypatch.setenv("LAKE_BACKEND", "databricks")
    assert lake.raw_dir() == "/Volumes/workspace/raw/olist"


def test_raw_dir_override(monkeypatch):
    monkeypatch.setenv("LAKE_RAW_DIR", "/tmp/csv")
    assert lake.raw_dir() == "/tmp/csv"


def test_local_write_is_a_delta_overwrite_by_path():
    df = MagicMock()
    lake.write_table(df, "bronze", "orders")
    df.write.format.assert_called_once_with("delta")
    df.write.format.return_value.mode.assert_called_once_with("overwrite")
    df.write.format.return_value.mode.return_value.save.assert_called_once_with("s3a://bronze/orders/")


def test_databricks_write_is_an_overwrite_into_a_table(monkeypatch):
    monkeypatch.setenv("LAKE_BACKEND", "databricks")
    df = MagicMock()
    lake.write_table(df, "bronze", "orders")
    df.write.mode.assert_called_once_with("overwrite")
    df.write.mode.return_value.saveAsTable.assert_called_once_with("workspace.bronze.orders")


def test_local_read_loads_a_delta_path():
    spark = MagicMock()
    lake.read_table(spark, "silver", "orders")
    spark.read.format.assert_called_once_with("delta")
    spark.read.format.return_value.load.assert_called_once_with("s3a://silver/orders/")


def test_databricks_read_uses_the_table_name(monkeypatch):
    monkeypatch.setenv("LAKE_BACKEND", "databricks")
    spark = MagicMock()
    lake.read_table(spark, "silver", "orders")
    spark.read.table.assert_called_once_with("workspace.silver.orders")


def test_databricks_does_not_stop_the_platform_session(monkeypatch):
    monkeypatch.setenv("LAKE_BACKEND", "databricks")
    spark = MagicMock()
    lake.stop_spark(spark)
    spark.stop.assert_not_called()


def test_local_stops_its_own_session():
    spark = MagicMock()
    lake.stop_spark(spark)
    spark.stop.assert_called_once()


def test_databricks_needs_no_bucket_creation(monkeypatch):
    """Sur Databricks, ensure_layers ne doit ni importer boto3 ni demander d'identifiants MinIO."""
    monkeypatch.setenv("LAKE_BACKEND", "databricks")
    monkeypatch.delenv("MINIO_ROOT_USER", raising=False)
    lake.ensure_layers(["bronze"])  # ne doit pas lever d'erreur


def test_databricks_notebook_is_valid_python():
    import ast
    from pathlib import Path

    notebook = Path(__file__).resolve().parents[1] / "databricks" / "run_batch.py"
    ast.parse(notebook.read_text(encoding="utf-8"))
    assert notebook.read_text(encoding="utf-8").startswith("# Databricks notebook source")
