# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline batch Olist sur Databricks (Free Edition, calcul serverless)
# MAGIC
# MAGIC Exécute Bronze -> Silver (+ quarantaine) -> Gold avec **le même code** que le pipeline local.
# MAGIC Seule la couche d'accès au stockage change (`src/common/lake.py`, backend `databricks`).
# MAGIC
# MAGIC Prérequis : schémas `bronze`, `silver`, `gold`, `quarantine`, `raw` et Volume `raw.olist`
# MAGIC contenant les 9 CSV (voir README). Le streaming Kafka reste en local.

# COMMAND ----------

# Chemin ABSOLU du dossier src/ de ton Git folder (le répertoire de travail n'est pas garanti
# sur le calcul serverless, les imports relatifs peuvent donc échouer).
# Exemple : /Workspace/Users/<ton-email>/olist-lakehouse/src
dbutils.widgets.text("repo_src", "")
REPO_SRC = dbutils.widgets.get("repo_src")
assert REPO_SRC, "Renseigne le widget repo_src avec le chemin absolu du dossier src/"

# COMMAND ----------

import os
import sys

# Pas de variables d'environnement au niveau du calcul sur serverless : on les fixe ici,
# AVANT d'importer le code du projet.
os.environ["LAKE_BACKEND"] = "databricks"
os.environ["LAKE_CATALOG"] = "workspace"

if REPO_SRC not in sys.path:
    sys.path.insert(0, REPO_SRC)

from batch import bronze_ingest, gold_star_schema, silver_quarantine

# COMMAND ----------

# MAGIC %md ## 1. Bronze : 9 CSV -> 9 tables Delta

# COMMAND ----------

bronze_ingest.main()

# COMMAND ----------

# MAGIC %md ## 2. Silver + quarantaine (porte de réconciliation bloquante)

# COMMAND ----------

silver_quarantine.main()

# COMMAND ----------

# MAGIC %md ## 3. Gold : étoile Kimball (contrôles d'intégrité bloquants)

# COMMAND ----------

gold_star_schema.main()

# COMMAND ----------

# MAGIC %md ## 4. Contrôle final : comparer avec les chiffres du pipeline local

# COMMAND ----------

display(
    spark.sql(
        """
        SELECT 'silver.orders' AS tbl, COUNT(*) AS n FROM workspace.silver.orders
        UNION ALL SELECT 'silver.order_items', COUNT(*) FROM workspace.silver.order_items
        UNION ALL SELECT 'gold.fact_order_items', COUNT(*) FROM workspace.gold.fact_order_items
        UNION ALL SELECT 'quarantine.orders', COUNT(*) FROM workspace.quarantine.orders
        """
    )
)
# Attendu (local) : silver.orders = 99441, silver.order_items = 112650,
# gold.fact_order_items = 112650, quarantine.orders = 0
