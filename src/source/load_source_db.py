"""Charge les CSV Olist dans PostgreSQL : le « système source » dont on captera les changements.

Pourquoi une vraie base source ? Un CSV ne change pas : il n'a ni UPDATE ni DELETE, donc rien
à capter. Une base transactionnelle, si : chaque modification passe par son journal (WAL), que
Debezium lira à la phase 2. Ce script crée les tables, les remplit une première fois
(« les enregistrements existants ») et prépare la base pour la capture des changements.

Idempotent : sans `--reset`, une table déjà remplie n'est pas touchée. On ne fait jamais de
`DROP TABLE` : une fois Debezium branché, supprimer une table casserait la réplication.
"""

import argparse
import csv

import psycopg2

from common.config import get_source_db_settings
from common.lake import raw_dir

# Colonnes dans l'ordre exact du CSV : `COPY` associe les colonnes par POSITION, pas par nom.
# Pas de clé étrangère : products et sellers ne sont pas (encore) dans la base source, et les
# suppressions en cascade seront faites par le générateur de changements, dans une transaction.
TABLES = {
    "orders": {
        "csv": "olist_orders_dataset.csv",
        "primary_key": ["order_id"],
        "columns": [
            ("order_id", "TEXT"),
            ("customer_id", "TEXT"),
            ("order_status", "TEXT"),
            ("order_purchase_timestamp", "TIMESTAMP"),
            ("order_approved_at", "TIMESTAMP"),
            ("order_delivered_carrier_date", "TIMESTAMP"),
            ("order_delivered_customer_date", "TIMESTAMP"),
            ("order_estimated_delivery_date", "TIMESTAMP"),
        ],
    },
    "customers": {
        "csv": "olist_customers_dataset.csv",
        "primary_key": ["customer_id"],
        "columns": [
            ("customer_id", "TEXT"),
            ("customer_unique_id", "TEXT"),
            ("customer_zip_code_prefix", "INTEGER"),
            ("customer_city", "TEXT"),
            ("customer_state", "TEXT"),
        ],
    },
    "order_items": {
        "csv": "olist_order_items_dataset.csv",
        "primary_key": ["order_id", "order_item_id"],
        "columns": [
            ("order_id", "TEXT"),
            ("order_item_id", "INTEGER"),
            ("product_id", "TEXT"),
            ("seller_id", "TEXT"),
            ("shipping_limit_date", "TIMESTAMP"),
            ("price", "NUMERIC(10,2)"),
            ("freight_value", "NUMERIC(10,2)"),
        ],
    },
}

# Table « témoin » : à la fin d'une série d'écritures, on y insère une ligne. Quand cette ligne
# apparaît dans le lakehouse, on sait que tout ce qui la précède dans le journal est arrivé.
MARKER_DDL = """
CREATE TABLE IF NOT EXISTS cdc_marker (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def create_table_sql(name: str, columns, primary_key) -> str:
    cols = ",\n    ".join(f"{col} {col_type}" for col, col_type in columns)
    return f"CREATE TABLE IF NOT EXISTS {name} (\n    {cols},\n    PRIMARY KEY ({', '.join(primary_key)})\n)"


def copy_sql(name: str, columns) -> str:
    """`COPY ... FROM STDIN` : chargement en bloc, bien plus rapide qu'un INSERT par ligne.

    En format CSV, une valeur vide non quotée devient NULL : c'est ce qu'on veut pour les
    dates de livraison absentes. `HEADER true` ignore la première ligne (et son éventuel BOM).
    """
    cols = ", ".join(col for col, _ in columns)
    return f"COPY {name} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)"


def count_csv_records(path: str) -> int:
    """Nombre d'enregistrements du CSV (en-tête exclu), avec le vrai parseur CSV, pas `wc -l`."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        return sum(1 for _ in csv.reader(f)) - 1


def _count_rows(cur, name: str) -> int:
    cur.execute(f"SELECT count(*) FROM {name}")
    return cur.fetchone()[0]


def load_table(cur, name: str, spec: dict, csv_dir: str, reset: bool = False) -> int:
    """Crée la table si besoin, la remplit depuis le CSV, retourne son nombre de lignes."""
    cur.execute(create_table_sql(name, spec["columns"], spec["primary_key"]))
    # FULL : sur UPDATE/DELETE, le journal contient l'ancienne ligne ENTIÈRE (sinon, seulement
    # sa clé). Debezium pourra alors fournir « avant » et « après » complets.
    cur.execute(f"ALTER TABLE {name} REPLICA IDENTITY FULL")

    existing = _count_rows(cur, name)
    if existing and not reset:
        print(f"{name} : déjà remplie ({existing} lignes), rien à faire (--reset pour recharger)")
        return existing
    if existing:
        cur.execute(f"TRUNCATE {name}")

    path = f"{csv_dir}/{spec['csv']}"
    expected = count_csv_records(path)
    with open(path, newline="", encoding="utf-8-sig") as f:
        cur.copy_expert(copy_sql(name, spec["columns"]), f)

    # Porte bloquante, comme ailleurs dans le projet : une ligne perdue = chargement cassé.
    loaded = _count_rows(cur, name)
    if loaded != expected:
        raise RuntimeError(f"{name} : {loaded} lignes chargées, {expected} attendues dans {spec['csv']}")
    print(f"{name} : {loaded} lignes chargées depuis {spec['csv']}")
    return loaded


def main():
    parser = argparse.ArgumentParser(description="Charge les CSV Olist dans PostgreSQL (base source)")
    parser.add_argument("--reset", action="store_true", help="vide et recharge les tables déjà remplies")
    args = parser.parse_args()

    conn = psycopg2.connect(**get_source_db_settings())
    try:
        # `with conn` = UNE transaction : tout est validé à la fin, ou rien (annulé si erreur).
        with conn, conn.cursor() as cur:
            for name, spec in TABLES.items():
                load_table(cur, name, spec, raw_dir(), reset=args.reset)
            cur.execute(MARKER_DDL)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
