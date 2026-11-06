"""Garde-fous légers sur les DAG, sans installer Airflow : on lit le code, on ne l'exécute pas."""

import ast
from pathlib import Path

DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"


def test_every_dag_file_is_valid_python():
    for f in DAGS.glob("*.py"):
        ast.parse(f.read_text(encoding="utf-8"), filename=str(f))


def test_pipeline_dag_keeps_its_safety_settings():
    src = (DAGS / "olist_pipeline.py").read_text(encoding="utf-8")
    assert "max_active_runs=1" in src  # un seul écrivain par table Delta
    assert "retries=" in src  # retries sûrs car jobs idempotents
    assert "execution_timeout=" in src
