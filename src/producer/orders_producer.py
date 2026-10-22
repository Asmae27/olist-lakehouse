import argparse
import csv
import json
import sys
import time

from confluent_kafka import Producer

BROKER = "kafka:19092"          # listener INTERNAL : on est dans le réseau Docker
TOPIC = "olist_orders"
CSV_PATH = "/opt/spark/work-dir/data/raw/olist_orders_dataset.csv"

stats = {"ok": 0, "failed": 0}


def on_delivery(err, msg):
    """Appelée par poll()/flush() une fois par message : succès ou échec."""
    if err is not None:
        stats["failed"] += 1
        print(f"Échec de livraison : {err}")
    else:
        stats["ok"] += 1


def load_orders(path, limit):
    # utf-8-sig : tolère un éventuel BOM, qui collerait sinon un caractère
    # invisible au nom de la première colonne (donc à la clé).
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    # Rejeu chronologique (les timestamps ISO se trient comme du texte).
    rows.sort(key=lambda r: r["order_purchase_timestamp"])
    return rows[:limit] if limit else rows


def main():
    parser = argparse.ArgumentParser(description="Rejoue les commandes Olist vers Kafka")
    parser.add_argument("--limit", type=int, default=100,
                        help="nombre de commandes à envoyer (0 = toutes)")
    parser.add_argument("--rate", type=float, default=50.0,
                        help="messages par seconde (0 = pas de limite)")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": BROKER,
        "client.id": "olist-orders-producer",
        "enable.idempotence": True,   # pas de doublon si un envoi est retenté
    })

    orders = load_orders(CSV_PATH, args.limit)
    interval = 1.0 / args.rate if args.rate > 0 else 0
    print(f"Envoi de {len(orders)} commandes vers {TOPIC} ({BROKER})")

    for row in orders:
        key = row["order_id"].encode("utf-8")
        value = json.dumps(row, ensure_ascii=False).encode("utf-8")
        while True:
            try:
                producer.produce(TOPIC, key=key, value=value, callback=on_delivery)
                break
            except BufferError:        # file d'attente locale pleine
                producer.poll(1)       # on laisse partir des messages, puis on réessaie
        producer.poll(0)               # déclenche les callbacks déjà prêts
        if interval:
            time.sleep(interval)

    remaining = producer.flush(30)     # attend la livraison de tout ce qui reste
    print(f"Terminé : {stats['ok']} confirmés, {stats['failed']} échecs, "
          f"{remaining} non livrés")
    sys.exit(0 if stats["failed"] == 0 and remaining == 0 else 1)


if __name__ == "__main__":
    main()