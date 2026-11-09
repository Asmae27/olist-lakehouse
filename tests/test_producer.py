"""Tests du producer Kafka et du chargement des commandes Olist."""

import csv
import json
import sys
from unittest.mock import MagicMock

import pytest

from producer import orders_producer


@pytest.fixture(autouse=True)
def reset_stats():
    """Évite que les statistiques d'un test contaminent les autres."""
    orders_producer.stats.update({"ok": 0, "failed": 0})


def _write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["order_id", "order_purchase_timestamp"],
        )
        writer.writeheader()
        writer.writerows(rows)


# --------------------------------------------------
# Tests de load_orders()
# --------------------------------------------------


def test_orders_are_replayed_in_purchase_order(tmp_path):
    p = tmp_path / "orders.csv"
    _write_csv(
        p,
        [
            {"order_id": "c", "order_purchase_timestamp": "2018-03-01"},
            {"order_id": "a", "order_purchase_timestamp": "2018-01-01"},
            {"order_id": "b", "order_purchase_timestamp": "2018-02-01"},
        ],
    )

    result = orders_producer.load_orders(p, skip=0, limit=0)

    assert [row["order_id"] for row in result] == ["a", "b", "c"]


def test_skip_and_limit_select_a_window(tmp_path):
    p = tmp_path / "orders.csv"
    _write_csv(
        p,
        [
            {
                "order_id": str(i),
                "order_purchase_timestamp": f"2018-01-{i:02d}",
            }
            for i in range(1, 11)
        ],
    )

    result = orders_producer.load_orders(p, skip=2, limit=3)

    assert [row["order_id"] for row in result] == ["3", "4", "5"]


def test_bom_is_stripped_from_first_column(tmp_path):
    p = tmp_path / "orders.csv"
    _write_csv(
        p,
        [{"order_id": "a", "order_purchase_timestamp": "2018-01-01"}],
    )

    result = orders_producer.load_orders(p, 0, 0)

    assert "order_id" in result[0]


def test_load_orders_returns_empty_list_for_empty_csv(tmp_path):
    p = tmp_path / "orders.csv"
    _write_csv(p, [])

    assert orders_producer.load_orders(p, 0, 0) == []


def test_load_orders_returns_empty_list_when_skip_exceeds_rows(tmp_path):
    p = tmp_path / "orders.csv"
    _write_csv(
        p,
        [{"order_id": "a", "order_purchase_timestamp": "2018-01-01"}],
    )

    assert orders_producer.load_orders(p, skip=5, limit=0) == []


def test_load_orders_propagates_file_not_found(tmp_path):
    p = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError):
        orders_producer.load_orders(p, 0, 0)


# --------------------------------------------------
# Tests de on_delivery()
# --------------------------------------------------


def test_on_delivery_increments_success_counter():
    orders_producer.on_delivery(None, MagicMock())

    assert orders_producer.stats["ok"] == 1
    assert orders_producer.stats["failed"] == 0


def test_on_delivery_increments_failure_counter():
    orders_producer.on_delivery("Kafka delivery error", MagicMock())

    assert orders_producer.stats["failed"] == 1
    assert orders_producer.stats["ok"] == 0


# --------------------------------------------------
# Tests de main()
# --------------------------------------------------


def test_main_sends_orders_successfully(tmp_path, monkeypatch):
    p = tmp_path / "orders.csv"
    _write_csv(
        p,
        [
            {"order_id": "order-1", "order_purchase_timestamp": "2018-01-01"},
            {"order_id": "order-2", "order_purchase_timestamp": "2018-01-02"},
        ],
    )

    producer_mock = MagicMock()
    producer_mock.flush.return_value = 0

    def fake_produce(topic, key, value, callback):
        callback(None, MagicMock())

    producer_mock.produce.side_effect = fake_produce

    monkeypatch.setattr(orders_producer, "Producer", MagicMock(return_value=producer_mock))
    monkeypatch.setattr(orders_producer, "CSV_PATH", str(p))
    monkeypatch.setattr(
        sys,
        "argv",
        ["orders_producer", "--limit", "0", "--rate", "0"],
    )

    with pytest.raises(SystemExit) as exc:
        orders_producer.main()

    assert exc.value.code == 0
    assert producer_mock.produce.call_count == 2
    assert orders_producer.stats["ok"] == 2
    assert orders_producer.stats["failed"] == 0
    producer_mock.flush.assert_called_once_with(30)

    # Vérifie le contenu JSON envoyé à Kafka.
    first_call = producer_mock.produce.call_args_list[0]
    assert first_call.args[0] == orders_producer.TOPIC
    assert first_call.kwargs["key"] == b"order-1"

    message = json.loads(first_call.kwargs["value"].decode("utf-8"))
    assert message["order_id"] == "order-1"


def test_main_retries_when_kafka_buffer_is_full(tmp_path, monkeypatch):
    p = tmp_path / "orders.csv"
    _write_csv(
        p,
        [{"order_id": "order-1", "order_purchase_timestamp": "2018-01-01"}],
    )

    producer_mock = MagicMock()
    producer_mock.flush.return_value = 0

    calls = {"count": 0}

    def fake_produce(topic, key, value, callback):
        calls["count"] += 1

        if calls["count"] == 1:
            raise BufferError("Local queue is full")

        callback(None, MagicMock())

    producer_mock.produce.side_effect = fake_produce

    monkeypatch.setattr(orders_producer, "Producer", MagicMock(return_value=producer_mock))
    monkeypatch.setattr(orders_producer, "CSV_PATH", str(p))
    monkeypatch.setattr(
        sys,
        "argv",
        ["orders_producer", "--limit", "0", "--rate", "0"],
    )

    with pytest.raises(SystemExit) as exc:
        orders_producer.main()

    assert exc.value.code == 0
    assert calls["count"] == 2
    assert producer_mock.poll.call_count >= 2


def test_main_exits_with_error_when_delivery_fails(tmp_path, monkeypatch):
    p = tmp_path / "orders.csv"
    _write_csv(
        p,
        [{"order_id": "order-1", "order_purchase_timestamp": "2018-01-01"}],
    )

    producer_mock = MagicMock()
    producer_mock.flush.return_value = 0

    def fake_produce(topic, key, value, callback):
        callback("Delivery failed", MagicMock())

    producer_mock.produce.side_effect = fake_produce

    monkeypatch.setattr(orders_producer, "Producer", MagicMock(return_value=producer_mock))
    monkeypatch.setattr(orders_producer, "CSV_PATH", str(p))
    monkeypatch.setattr(
        sys,
        "argv",
        ["orders_producer", "--limit", "0", "--rate", "0"],
    )

    with pytest.raises(SystemExit) as exc:
        orders_producer.main()

    assert exc.value.code == 1
    assert orders_producer.stats["failed"] == 1


def test_main_exits_with_error_when_messages_remain_undelivered(tmp_path, monkeypatch):
    p = tmp_path / "orders.csv"
    _write_csv(
        p,
        [{"order_id": "order-1", "order_purchase_timestamp": "2018-01-01"}],
    )

    producer_mock = MagicMock()
    producer_mock.flush.return_value = 1

    def fake_produce(topic, key, value, callback):
        callback(None, MagicMock())

    producer_mock.produce.side_effect = fake_produce

    monkeypatch.setattr(orders_producer, "Producer", MagicMock(return_value=producer_mock))
    monkeypatch.setattr(orders_producer, "CSV_PATH", str(p))
    monkeypatch.setattr(
        sys,
        "argv",
        ["orders_producer", "--limit", "0", "--rate", "0"],
    )

    with pytest.raises(SystemExit) as exc:
        orders_producer.main()

    assert exc.value.code == 1
    assert orders_producer.stats["ok"] == 1
