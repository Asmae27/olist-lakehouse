"""Accès au stockage du lakehouse, indépendant de la plateforme.

Les jobs batch ne savent pas où vivent les données : ils demandent « la table `orders` de
la couche `silver` ». Ce module traduit selon le backend choisi par `LAKE_BACKEND` :

- `local` (défaut) : tables Delta par chemin dans MinIO, `s3a://silver/orders/`.
- `databricks`     : tables Unity Catalog, `workspace.silver.orders`.

La logique métier reste identique ; seule cette couche d'accès change.
"""

import os

BACKENDS = ("local", "databricks")


def get_backend() -> str:
    backend = os.environ.get("LAKE_BACKEND", "local")
    if backend not in BACKENDS:
        raise ValueError(f"LAKE_BACKEND doit valoir l'une de {BACKENDS}, reçu : {backend!r}")
    return backend


def _catalog() -> str:
    return os.environ.get("LAKE_CATALOG", "workspace")


def table_ref(layer: str, name: str) -> str:
    """Identifiant de la table : chemin S3 en local, nom à trois niveaux sur Databricks."""
    if get_backend() == "databricks":
        return f"{_catalog()}.{layer}.{name}"
    return f"s3a://{layer}/{name}/"


def read_table(spark, layer: str, name: str):
    if get_backend() == "databricks":
        return spark.read.table(table_ref(layer, name))
    return spark.read.format("delta").load(table_ref(layer, name))


def write_table(df, layer: str, name: str) -> None:
    """Réécrit entièrement la table (overwrite) : idempotent, donc rejouable sans risque."""
    if get_backend() == "databricks":
        df.write.mode("overwrite").saveAsTable(table_ref(layer, name))
    else:
        df.write.format("delta").mode("overwrite").save(table_ref(layer, name))


def raw_dir() -> str:
    """Dossier des CSV d'entrée. `LAKE_RAW_DIR` permet de le remplacer."""
    default = (
        f"/Volumes/{_catalog()}/raw/olist"
        if get_backend() == "databricks"
        else "/opt/spark/work-dir/data/raw"
    )
    return os.environ.get("LAKE_RAW_DIR", default)


def ensure_layers(layers) -> None:
    """Prépare le stockage. En local : crée les buckets MinIO manquants.
    Sur Databricks : rien à faire, les schémas sont créés une fois pour toutes (voir README)."""
    if get_backend() == "local":
        from common.storage import ensure_buckets_exist  # import tardif : boto3 inutile sur Databricks

        ensure_buckets_exist(layers)


def get_spark(app_name: str):
    """SparkSession configurée pour MinIO en local ; session déjà active sur Databricks."""
    if get_backend() == "databricks":
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()
    from common.spark_session import get_spark_session

    return get_spark_session(app_name)


def stop_spark(spark) -> None:
    """Sur Databricks la session est gérée par la plateforme : on ne l'arrête pas."""
    if get_backend() == "local":
        spark.stop()
