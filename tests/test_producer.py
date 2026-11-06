"""Le producer : le rejeu doit être déterministe (tri par date) et découpable (skip/limit)."""

import csv

from producer.orders_producer import load_orders


def _write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["order_id", "order_purchase_timestamp"])
        w.writeheader()
        w.writerows(rows)


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
    assert [r["order_id"] for r in load_orders(p, skip=0, limit=0)] == ["a", "b", "c"]


def test_skip_and_limit_select_a_window(tmp_path):
    p = tmp_path / "orders.csv"
    _write_csv(
        p, [{"order_id": str(i), "order_purchase_timestamp": f"2018-01-{i:02d}"} for i in range(1, 11)]
    )
    window = load_orders(p, skip=2, limit=3)
    assert [r["order_id"] for r in window] == ["3", "4", "5"]


def test_bom_is_stripped_from_first_column(tmp_path):
    p = tmp_path / "orders.csv"
    _write_csv(p, [{"order_id": "a", "order_purchase_timestamp": "2018-01-01"}])
    assert "order_id" in load_orders(p, 0, 0)[0]
