"""Les secrets viennent de l'environnement : sans eux, le code doit refuser de démarrer."""

import pytest

from common.config import get_env, get_minio_endpoint
from common.spark_session import get_spark_session
from common.storage import ensure_buckets_exist


def test_get_env_returns_the_value(monkeypatch):
    monkeypatch.setenv("MINIO_ROOT_USER", "alice")
    assert get_env("MINIO_ROOT_USER") == "alice"


def test_get_env_fails_loudly_when_missing(monkeypatch):
    monkeypatch.delenv("MINIO_ROOT_USER", raising=False)
    with pytest.raises(RuntimeError, match="MINIO_ROOT_USER"):
        get_env("MINIO_ROOT_USER")


def test_empty_value_counts_as_missing(monkeypatch):
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "")
    with pytest.raises(RuntimeError):
        get_env("MINIO_ROOT_PASSWORD")


def test_endpoint_has_a_default_but_can_be_overridden(monkeypatch):
    monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
    assert get_minio_endpoint() == "http://minio:9000"
    monkeypatch.setenv("MINIO_ENDPOINT", "http://localhost:9000")
    assert get_minio_endpoint() == "http://localhost:9000"


def test_spark_session_refuses_to_start_without_credentials(monkeypatch):
    monkeypatch.delenv("MINIO_ROOT_USER", raising=False)
    monkeypatch.delenv("MINIO_ROOT_PASSWORD", raising=False)
    with pytest.raises(RuntimeError, match="MINIO_ROOT_USER"):
        get_spark_session("test")


def test_bucket_helper_refuses_to_start_without_credentials(monkeypatch):
    monkeypatch.delenv("MINIO_ROOT_USER", raising=False)
    monkeypatch.delenv("MINIO_ROOT_PASSWORD", raising=False)
    with pytest.raises(RuntimeError, match="MINIO_ROOT_USER"):
        ensure_buckets_exist(["bronze"])