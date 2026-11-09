# Lakehouse Olist en streaming

![CI](https://github.com/Asmae27/olist-lakehouse/actions/workflows/ci.yml/badge.svg)

Projet de portfolio en Data Engineering : architecture lakehouse (médaillon) construite autour du dataset public Olist (e-commerce brésilien), avec ingestion batch et streaming, contrôles qualité (quarantaine et portes bloquantes), modèle en étoile Kimball, orchestration Airflow, intégration continue GitHub Actions et portage de la partie batch sur Databricks.

**État au 09/11/2026** : le pipeline complet est orchestré par Airflow et s'exécute de bout en bout en local : ingestion batch des CSV et ingestion en streaming depuis Kafka (en parallèle), Silver avec quarantaine, upsert du flux dans Silver, puis Gold en étoile. Une CI (lint + tests unitaires) vérifie chaque push, et les secrets ne sont plus dans le code. La partie batch a aussi été portée sur Databricks Free Edition avec le même code métier : les chiffres de contrôle sont identiques (voir [Batch sur Databricks](#batch-sur-databricks-free-edition)). Il reste à planifier le DAG et à brancher une alerte externe sur le taux de quarantaine (voir [Avancement](#avancement)).

## Architecture

### Vue d'ensemble

```mermaid
flowchart LR
    subgraph SRC["Sources"]
        CSV[("9 CSV Olist")]
        PROD["Producer Python<br/>confluent-kafka<br/>clé = order_id"]
    end

    subgraph LOCAL["Local : Docker Compose sur WSL2"]
        KAFKA["Kafka KRaft<br/>topic olist_orders<br/>3 partitions"]
        STREAM["Spark Structured Streaming<br/>availableNow + checkpoint"]
        AF{{"Airflow<br/>DAG olist_pipeline<br/>LocalExecutor"}}
        subgraph MINIO["MinIO (S3) : tables Delta"]
            BRZ[("Bronze batch<br/>9 tables")]
            BRZS[("Bronze flux<br/>orders_stream")]
            SLV[("Silver<br/>6 tables")]
            QRT[("Quarantaine<br/>avec quarantine_reason")]
            GLD[("Gold<br/>étoile Kimball")]
        end
    end

    subgraph DBX["Databricks Free Edition : serverless + Unity Catalog"]
        VOL[("Volume<br/>raw.olist")]
        NB["Notebook<br/>run_batch.py"]
        UC[("Tables workspace<br/>bronze, silver, gold, quarantine")]
    end

    subgraph DEV["Développement"]
        GH["GitHub : olist-lakehouse<br/>src/batch + common/lake.py<br/>un seul code, deux backends"]
        CI["GitHub Actions<br/>ruff + 60 tests"]
    end

    CSV -->|"bronze_ingest"| BRZ
    PROD --> KAFKA --> STREAM -->|"stream_to_bronze"| BRZS
    BRZ -->|"silver_quarantine"| SLV
    BRZ -->|"lignes rejetées"| QRT
    BRZS -->|"MERGE sur order_id"| SLV
    SLV -->|"gold_star_schema"| GLD

    AF -.->|"orchestre les jobs Spark"| MINIO
    AF -.-> STREAM

    CSV -->|"téléversement"| VOL
    VOL --> NB --> UC

    GH --> CI
    GH -->|"git clone"| LOCAL
    GH -->|"Git folder"| NB

    classDef store fill:#dbeafe,stroke:#1d4ed8,color:#111827
    classDef job fill:#dcfce7,stroke:#15803d,color:#111827
    classDef orch fill:#fef3c7,stroke:#b45309,color:#111827
    classDef dev fill:#f3e8ff,stroke:#7e22ce,color:#111827
    class BRZ,BRZS,SLV,QRT,GLD,UC,VOL,CSV store
    class KAFKA,STREAM,PROD,NB job
    class AF orch
    class GH,CI dev
```

**Lecture du schéma**

- **Flèches pleines** : circulation des données. **Flèches pointillées** : orchestration (Airflow déclenche les jobs Spark par `docker exec`).
- **Branche batch** : les 9 CSV passent par Bronze, puis Silver (avec quarantaine), puis Gold. Elle s'exécute en local **et** sur Databricks, avec le même code.
- **Branche streaming** : producer, Kafka (clé = `order_id`, 3 partitions), Structured Streaming (checkpoint, exactement une fois), puis `MERGE` dans Silver. Elle reste **uniquement en local** : Databricks Free Edition ne peut ni installer le connecteur Kafka ni joindre un Kafka local.
- **Portes qualité** (bloquantes) : `silver + quarantaine = bronze` après Silver, puis égalité des lignes de la table de faits et absence de clé orpheline après Gold. Une ligne en quarantaine n'est pas un échec : elle signale un problème de source.
- **Développement** : chaque push déclenche la CI (lint et tests unitaires). Le même dépôt est cloné en local et lu par le Git folder de Databricks.

### Deux plateformes, un seul code batch

La partie batch (Bronze, Silver, Gold) tourne en local sur MinIO et sur Databricks. Les jobs ne connaissent pas l'emplacement des données : ils passent par `src/common/lake.py`, qui traduit « la table `orders` de la couche `silver` » selon la plateforme.

```mermaid
flowchart TB
    CODE["src/batch/*.py<br/>logique métier unique"] --> LAKE["common/lake.py<br/>accès au stockage"]
    LAKE -->|"LAKE_BACKEND=local (défaut)"| LOCAL["MinIO : tables Delta par chemin<br/>s3a://silver/orders/"]
    LAKE -->|"LAKE_BACKEND=databricks"| DBX["Unity Catalog : tables gérées<br/>workspace.silver.orders"]
```

Le streaming Kafka reste uniquement en local (voir [Batch sur Databricks](#batch-sur-databricks-free-edition)).

### Orchestration (DAG `olist_pipeline`)

```mermaid
flowchart LR
    A[bronze_ingest] --> B[silver_quarantine]
    C[stream_to_bronze] --> D[silver_orders_from_stream]
    B --> D
    D --> E[gold_star_schema]
```

- `bronze_ingest` et `stream_to_bronze` démarrent en parallèle (branches indépendantes).
- `silver_orders_from_stream` attend `silver_quarantine` : celui-ci **réécrit** `silver/orders` en entier, le `MERGE` doit donc venir après, sinon il serait écrasé.
- `gold_star_schema` attend les deux, car il lit les six tables Silver.
- Un second DAG, `olist_orders_source_simulator`, simule la source externe : il envoie un lot de commandes dans Kafka (paramètres `skip` et `limit`, déclenchement manuel).

Options d'évolution : remplacer MinIO par un bucket S3/GCS et tester la charge sur un jeu plus volumineux (NYC TLC).

## Stack technique

| Composant | Version |
|---|---|
| Apache Spark | 3.5.9 (Scala 2.12, Java 11) |
| Delta Lake | 3.3.3 (`delta-spark_2.12` + `delta-storage`) |
| Hadoop / hadoop-aws | 3.3.4 |
| aws-java-sdk-bundle | 1.12.262 |
| Connecteur Kafka de Spark | `spark-sql-kafka-0-10_2.12:3.5.9` (via `--packages`) |
| Stockage objet | fork `pgsty/silo:RELEASE.2026-09-16T00-00-00Z` (compatible S3/MinIO) |
| Kafka | `apache/kafka:4.4.0` (mode KRaft, un nœud) |
| Client Kafka Python | `confluent-kafka==2.16.0` |
| Airflow | 3.3.2, image personnalisée `olist-airflow:3.3.2` (ajoute le client Docker 28.5.1), `LocalExecutor`, PostgreSQL 16 |
| Great Expectations | série 0.18.x (`SparkDFDataset`) |
| boto3 | création des buckets (version non épinglée) |
| CI | GitHub Actions, `ruff` 0.17.3, `pytest` 8.4.2 |
| Databricks | Free Edition, calcul serverless, Unity Catalog (catalogue `workspace`), Git folder |

Les versions sont épinglées volontairement (voir [Décisions](#décisions-et-compromis)).

## Structure du dépôt

```
.
├── .github/workflows/ci.yml    # CI : lint (ruff) + tests (pytest)
├── .env.example                # modèle des variables d'environnement (MinIO)
├── .gitattributes              # fins de ligne LF imposées
├── pyproject.toml              # configuration de ruff et de pytest
├── docker-compose.yml          # services minio + spark + kafka
├── docker/spark/Dockerfile     # image Spark custom (dépendances Python)
├── docker/spark/jars/          # jars Spark (non versionnés, voir Setup)
├── requirements.txt            # dépendances Python installées dans l'image Spark
├── requirements-dev.txt        # outils de développement : pyspark, pytest, ruff
├── data/raw/                   # CSV Olist (non versionnés)
├── databricks/
│   └── run_batch.py            # notebook Databricks : Bronze -> Silver -> Gold + contrôle final
├── airflow/                    # projet Compose séparé : Airflow
│   ├── .env.example                # modèle des variables d'Airflow (secrets, UID, GID)
│   ├── Dockerfile                  # image Airflow + client Docker
│   ├── docker-compose.yaml         # compose officiel 3.3.2, simplifié (LocalExecutor)
│   ├── docker-compose.override.yaml# socket Docker monté dans le scheduler seulement
│   └── dags/
│       ├── hello_pipeline.py               # DAG de démonstration
│       ├── olist_pipeline.py               # pipeline complet
│       └── olist_orders_source_simulator.py# simulateur de source (producer)
├── tests/                      # tests unitaires (sans infrastructure)
│   ├── conftest.py                 # fixture SparkSession locale
│   ├── test_quarantine_rules.py    # règles qualité Silver
│   ├── test_silver_quarantine_process.py # orchestration d'une table Silver (écritures, alerte, réconciliation)
│   ├── test_stream_rules.py        # règle « date malformée » du flux
│   ├── test_producer.py            # rejeu du producer
│   ├── test_dag_files.py           # garde-fous sur les DAG
│   ├── test_config.py              # lecture des secrets depuis l'environnement
│   ├── test_lake.py                # routage du stockage selon le backend
│   └── test_storage.py             # création des buckets MinIO (client S3 simulé)
└── src/
    ├── common/
    │   ├── config.py           # lecture des variables d'environnement (échec immédiat si absente)
    │   ├── lake.py             # accès au stockage indépendant de la plateforme (local / Databricks)
    │   ├── spark_session.py    # SparkSession locale (config S3A + Delta)
    │   └── storage.py          # création des buckets MinIO manquants (sans conflit entre jobs parallèles)
    ├── producer/
    │   └── orders_producer.py  # rejoue les commandes vers Kafka (clé = order_id)
    ├── streaming/
    │   ├── kafka_to_console.py          # lecture Kafka -> console (validation du connecteur)
    │   ├── orders_stream_to_bronze.py   # Kafka -> bronze/orders_stream (Delta)
    │   └── silver_orders_from_stream.py # bronze/orders_stream -> silver/orders (MERGE)
    └── batch/
        ├── bronze_ingest.py        # CSV -> Bronze (9 tables Delta)
        ├── silver_quarantine.py    # Bronze -> Silver + Quarantaine
        ├── gold_star_schema.py     # Silver -> Gold (étoile Kimball)
        ├── validate_orders.py      # contrôles Great Expectations sur orders
        └── debug_reviews.py        # diagnostic de lecture CSV (order_reviews)
```

## Prérequis

- Docker + Docker Compose (v2.20 ou plus)
- WSL2 si Windows, avec 14 Go de RAM alloués pendant le développement (voir `.wslconfig`) : Spark, Kafka, MinIO et Airflow tournent ensemble
- Environ 300 Mo de disque pour les jars Spark/S3 (non versionnés), plus les images Docker
- Pour lancer les tests en local (hors Docker) : Python 3.12 et Java 17 (voir [Tests et intégration continue](#tests-et-intégration-continue))
- Pour la partie Databricks (facultative) : un compte Databricks Free Edition (voir [Batch sur Databricks](#batch-sur-databricks-free-edition))

## Setup

1. Cloner le dépôt :

   ```bash
   git clone https://github.com/Asmae27/olist-lakehouse.git
   cd olist-lakehouse
   ```

2. Télécharger les jars Spark (non versionnés, voir Décisions) :

   ```bash
   mkdir -p docker/spark/jars && cd docker/spark/jars
   wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
   wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
   wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.3/delta-spark_2.12-3.3.3.jar
   wget https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.3/delta-storage-3.3.3.jar
   cd ../../..
   ```

3. Télécharger le dataset « Brazilian E-Commerce Public Dataset by Olist » sur Kaggle et placer les 9 CSV dans `data/raw/`.

4. Créer le fichier `.env` à la racine (ignoré par Git) à partir du modèle, puis choisir les identifiants de MinIO :

   ```bash
   cp .env.example .env
   # éditer .env : MINIO_ROOT_USER et MINIO_ROOT_PASSWORD (8 caractères minimum)
   ```

   Docker Compose lit ce fichier automatiquement. Sans ces deux variables, `docker compose up` échoue avec un message explicite, et les jobs Spark refusent de démarrer (voir [Secrets](#secrets-et-configuration)).

5. Construire l'image Spark et démarrer les services :

   ```bash
   docker compose build spark
   docker compose up -d
   docker compose ps
   ```

   `minio` doit être `healthy`, `kafka` et `spark` `running`. Si un job échoue avec « Variable d'environnement manquante », le conteneur `spark` n'a pas reçu les variables : vérifier avec `docker exec spark env | grep MINIO`, puis recréer le conteneur avec `docker compose up -d` (un conteneur déjà lancé garde son ancien environnement).

6. Créer le topic Kafka (une seule fois) :

   ```bash
   docker compose exec --workdir /opt/kafka/bin kafka \
     ./kafka-topics.sh --bootstrap-server kafka:19092 \
     --create --topic olist_orders --partitions 3 --replication-factor 1
   ```

7. Console MinIO : http://localhost:9001, avec les identifiants choisis dans `.env`.

### Airflow (projet Compose séparé)

```bash
cd airflow
docker build -t olist-airflow:3.3.2 .
mkdir -p dags logs plugins config
cp .env.example .env
```

Compléter `airflow/.env` (ignoré par Git). Chaque variable est décrite dans `airflow/.env.example`, avec la commande qui la génère :

```
AIRFLOW_UID=<sortie de `id -u`>
AIRFLOW_IMAGE_NAME=olist-airflow:3.3.2
DOCKER_GID=<groupe du socket Docker, voir ci-dessous>
FERNET_KEY=<clé générée>
AIRFLOW__API_AUTH__JWT_SECRET=<secret généré>
AIRFLOW__API__SECRET_KEY=<secret généré>
```

- `FERNET_KEY` chiffre les connexions et variables stockées en base : `python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`. Ne pas la changer une fois Airflow initialisé, et ne pas la partager.
- `AIRFLOW__API_AUTH__JWT_SECRET` signe les jetons de l'API, `AIRFLOW__API__SECRET_KEY` signe les sessions de l'interface : `openssl rand -base64 32` pour chacun.
- Trouver le groupe du socket Docker (il dépend de l'installation de Docker Desktop) :
  `docker run --rm -v /var/run/docker.sock:/var/run/docker.sock alpine ls -ln /var/run/docker.sock` ; le groupe est le numéro de la deuxième colonne après `1`.

Le dossier `airflow/config/` contient un `airflow.cfg` généré au premier démarrage, avec des secrets : il est ignoré par Git et ne doit pas être versionné.

Puis :

```bash
docker compose up airflow-init     # migrations de la base, création de l'utilisateur
docker compose up -d
docker compose ps                  # tous les services en healthy
```

Interface : http://localhost:8080 (compte par défaut `airflow` / `airflow`, acceptable uniquement en local : ne jamais exposer ce port tel quel).

## Exécuter le pipeline

### Avec Airflow (recommandé)

1. Dans l'interface, déclencher `olist_orders_source_simulator` (paramètres `skip` et `limit`, par défaut 0 et 100) : il envoie un lot de commandes dans Kafka.
2. Déclencher `olist_pipeline` : il traite le batch, lit les nouveaux messages Kafka, met Silver à jour et reconstruit Gold. Un seul run actif à la fois (`max_active_runs=1`).
3. Pour envoyer d'autres commandes, relancer le simulateur avec un `skip` plus grand (par exemple 100) : le producer rejoue toujours depuis le début du fichier trié, sans mémoire.

Pour repartir de zéro, supprimer **à la fois** `bronze/orders_stream/`, `checkpoints/orders_stream/` et `quarantine/orders_stream/` dans MinIO (un checkpoint sans sa table, ou l'inverse, est incohérent), puis supprimer et recréer le topic.

### À la main

Les scripts s'exécutent dans l'ordre depuis le conteneur Spark. Les buckets MinIO manquants sont créés automatiquement.

```bash
# Branche batch
docker compose exec spark spark-submit /opt/spark/work-dir/src/batch/bronze_ingest.py
docker compose exec spark spark-submit /opt/spark/work-dir/src/batch/silver_quarantine.py

# Branche streaming
docker compose exec spark python3 /opt/spark/work-dir/src/producer/orders_producer.py --skip 0 --limit 100 --rate 50
docker compose exec spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/work-dir/src/streaming/orders_stream_to_bronze.py
docker compose exec spark spark-submit /opt/spark/work-dir/src/streaming/silver_orders_from_stream.py

# Gold
docker compose exec spark spark-submit /opt/spark/work-dir/src/batch/gold_star_schema.py
```

`silver_quarantine.py` réécrit entièrement `silver/orders` : il doit donc passer **avant** l'upsert du flux (sinon les lignes issues du flux seraient écrasées).

## Batch sur Databricks Free Edition

Bronze, Silver (avec quarantaine) et Gold s'exécutent aussi sur Databricks, avec **exactement le même code** que le pipeline local. Seul le backend de stockage change : les tables Delta par chemin dans MinIO deviennent des tables gérées Unity Catalog.

### Ce qui est porté, et ce qui ne l'est pas

| Élément | Où | Pourquoi |
|---|---|---|
| Bronze, Silver + quarantaine, Gold (batch) | local **et** Databricks | code identique, backend choisi par `LAKE_BACKEND` |
| Kafka, producer, Structured Streaming, `MERGE` du flux | local uniquement | le calcul serverless n'accepte pas les paquets Maven (donc pas le connecteur Kafka) et ne peut pas joindre un Kafka local |
| Orchestration | Airflow (local) ; manuelle sur Databricks | pas de DAG ni de job planifié côté Databricks pour l'instant |

Conséquence : sur Databricks, `silver.orders` ne reçoit que le batch, sans l'upsert du flux.

### Mise en place (une seule fois)

1. Créer un compte **Databricks Free Edition** (calcul serverless, Unity Catalog, catalogue `workspace` fourni).
2. Créer les schémas et le Volume, dans l'éditeur SQL :

   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.bronze;
   CREATE SCHEMA IF NOT EXISTS workspace.silver;
   CREATE SCHEMA IF NOT EXISTS workspace.gold;
   CREATE SCHEMA IF NOT EXISTS workspace.quarantine;
   CREATE SCHEMA IF NOT EXISTS workspace.raw;

   CREATE VOLUME IF NOT EXISTS workspace.raw.olist;
   ```

3. Téléverser les 9 CSV Olist dans le Volume (**New → Add or upload data → Upload files to volume**), sans les renommer : `bronze_ingest.py` déduit le nom des tables de celui des fichiers. Chemin final : `/Volumes/workspace/raw/olist/`.
4. Cloner le dépôt : **Workspace → Create → Git folder**, avec l'URL `https://github.com/Asmae27/olist-lakehouse` (le clonage a fonctionné sans jeton, le dépôt étant public). Après chaque push, faire un **Pull** dans le Git folder.

### Exécution

1. Ouvrir `databricks/run_batch.py` dans le Git folder (il s'affiche comme un notebook).
2. Renseigner le champ `repo_src` avec le chemin **absolu** du dossier `src/` : `/Workspace/Users/<ton-email>/olist-lakehouse/src`.
3. Attacher le notebook au calcul **Serverless** et exécuter les cellules (ou *Run all*).

Le notebook fixe lui-même `LAKE_BACKEND=databricks` et `LAKE_CATALOG=workspace` avant d'importer le code (pas de variables d'environnement au niveau du calcul sur serverless), ajoute `src/` au chemin de recherche Python, puis appelle `main()` de chaque job dans l'ordre. Les portes qualité sont les mêmes qu'en local : une réconciliation ou un contrôle d'intégrité en échec lève une erreur et arrête le notebook.

### Résultats du run du 09/11/2026

| Étape | Résultat |
|---|---|
| Bronze | 9 tables, dont 99 441 commandes, 99 224 avis (identique au local) et 1 000 163 lignes de géolocalisation |
| Silver | 6 tables, réconciliation vérifiée (`Silver + quarantaine = Bronze`), 0 ligne en quarantaine |
| Gold | `fact_order_items` : 112 650 lignes, égal à `silver/order_items` ; 0 clé orpheline sur les 4 clés étrangères ; plage de dates 2016-09-04 → 2018-10-17 |

Contrôle final, comparé aux chiffres du pipeline local :

| Table | Databricks | Local |
|---|---|---|
| `silver.orders` | 99 441 | 99 441 |
| `silver.order_items` | 112 650 | 112 650 |
| `gold.fact_order_items` | 112 650 | 112 650 |
| `quarantine.orders` | 0 | 0 |

Durées affichées par l'interface : Bronze environ 1 min (arrondi par l'interface), Silver 45 s, Gold 24 s, contrôle final 5 s, soit environ 2 min 15 de calcul. Ces durées sont indicatives et **non comparables** aux 5 min 40 du pipeline local : sur Databricks, les trois jobs partagent une même session Spark sur du calcul managé, alors qu'en local chaque tâche démarre sa propre session dans un conteneur, sur un portable. Elles proviennent d'un seul run, pas d'un benchmark.

### Configuration du stockage

| Variable | Valeur par défaut | Rôle |
|---|---|---|
| `LAKE_BACKEND` | `local` | `local` (MinIO, chemins `s3a://`) ou `databricks` (tables Unity Catalog) |
| `LAKE_CATALOG` | `workspace` | catalogue Unity Catalog (backend `databricks`) |
| `LAKE_RAW_DIR` | selon le backend | dossier des CSV d'entrée (`/opt/spark/work-dir/data/raw` en local, `/Volumes/workspace/raw/olist` sur Databricks) |

## Tests et intégration continue

### Ce que la CI vérifie

À chaque push sur `main` et à chaque pull request, GitHub Actions lance deux jobs indépendants (`.github/workflows/ci.yml`) :

| Job | Commande | Ce qu'il détecte |
|---|---|---|
| `lint` | `ruff check .` puis `ruff format --check .` | imports inutilisés ou manquants, variables non définies, pièges Python courants, style incohérent |
| `test` | `pytest -v` (Java 17 + Python 3.12) | régression de la logique de qualité, du producer, des réglages de sécurité des DAG, de la gestion des secrets, du routage du stockage |

### Les tests

60 tests, qui s'exécutent sur une SparkSession locale, **sans MinIO, Kafka ni Delta** : ils vérifient la logique des DataFrames, pas l'infrastructure. Les accès au stockage (client S3, lecture et écriture des tables) sont remplacés par des objets simulés.

| Fichier | Protège |
|---|---|
| `test_quarantine_rules.py` | la raison de quarantaine est correcte, une ligne qui échoue à plusieurs règles les liste toutes, et `valides + quarantaine == total` (l'invariant de la porte bloquante) |
| `test_stream_rules.py` | une date **absente** (vide, `null` ou faite d'espaces) est légitime et reste en Silver ; une date valide n'est pas mise en quarantaine ; une date illisible l'est, avec une règle dédiée par colonne de date, nommée `<colonne>_malformed` |
| `test_producer.py` | **lecture** : le rejeu est trié par date d'achat, `skip` et `limit` découpent une fenêtre, le BOM du CSV est retiré, un CSV vide ou un `skip` supérieur au nombre de lignes donne une liste vide, un fichier absent lève `FileNotFoundError` ; **envoi** : le callback de livraison compte les succès et les échecs, `main` envoie les commandes, retente quand le tampon d'envoi de Kafka est plein, et se termine avec un code d'erreur si une livraison échoue ou si des messages restent non livrés (pour qu'Airflow voie la tâche en échec) |
| `test_dag_files.py` | les DAG sont du Python valide et gardent `max_active_runs=1`, `retries` et `execution_timeout` |
| `test_silver_quarantine_process.py` | `process_table` écrit les lignes valides en `silver` et les rejetées en `quarantine`, avec la bonne raison ; une alerte qualité est émise quand le taux dépasse le seuil (50 % dans le test) et pas sur une table vide ; **si la réconciliation échoue, une erreur est levée et rien n'est écrit** ; `main` initialise le stockage, traite toutes les tables dans l'ordre des règles, puis arrête la session |
| `test_config.py` | le code refuse de démarrer sans identifiants MinIO, avec un message qui nomme la variable manquante |
| `test_storage.py` | `ensure_buckets_exist` ne crée rien si tout existe, crée uniquement les buckets manquants, **ignore l'erreur « bucket déjà créé »** (cas de deux jobs parallèles qui créent le même bucket), propage toute autre erreur (par exemple `AccessDenied`), et configure le client S3 (adresse, identifiants lus dans l'environnement, région, signature v4) |
| `test_lake.py` | chaque backend vise le bon endroit (chemin `s3a://` ou nom `catalogue.schéma.table`), utilise la bonne méthode de lecture et d'écriture, n'arrête pas la session de la plateforme, n'exige aucun identifiant MinIO sur Databricks ; le notebook est du Python valide |

### Lancer les tests en local

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements-dev.txt      # inclut requirements.txt + pyspark, pytest, ruff
ruff check . && ruff format --check .    # lint
pytest -v                                # tests (Java requis : sudo apt install openjdk-17-jre-headless)
```

Pour corriger automatiquement les remarques de style : `ruff check --fix . && ruff format .`. Refaire ces commandes après avoir copié ou modifié des fichiers : c'est exactement ce que fait la CI, et cela évite un aller-retour.

### Ce que la CI ne vérifie pas

Elle ne démarre ni MinIO, ni Kafka, ni Airflow, et ne se connecte pas à Databricks : elle ne prouve donc pas que la stack fonctionne de bout en bout. Cette validation d'intégration reste le rôle d'un run complet du DAG `olist_pipeline` en local, et du notebook `run_batch.py` sur Databricks, tous deux contrôlés par leurs portes qualité.

## Secrets et configuration

- Les secrets vivent dans des fichiers `.env` **non versionnés** (`.env` à la racine, `airflow/.env`). Les fichiers `.env.example` documentent les variables attendues, sans valeur réelle.
- Dans le code Python, `common/config.py` lit les variables d'environnement. Il n'y a **pas de valeur par défaut pour un secret** : s'il manque, le job s'arrête tout de suite avec un message explicite, au lieu de tourner avec un identifiant oublié. L'adresse de MinIO (`MINIO_ENDPOINT`, par défaut `http://minio:9000`) n'est pas un secret et garde un défaut.
- Les variables sont lues dans les fonctions, pas à l'import : la CI peut importer les modules sans disposer d'aucun secret.
- Dans Docker Compose, la syntaxe `${VARIABLE:?message}` fait échouer `docker compose up` si une variable obligatoire manque.
- Les jobs Spark lancés par Airflow (`docker exec`) héritent de l'environnement du conteneur `spark` : c'est là que les identifiants MinIO sont injectés.
- Sur Databricks, aucun identifiant MinIO n'est nécessaire : le backend `databricks` n'importe ni `boto3` ni le module de configuration des identifiants.

## Modèle de données

### Médaillon

| Couche | Contenu |
|---|---|
| Bronze | 9 tables Delta (une par CSV source, sans transformation métier) + `orders_stream` (messages Kafka parsés, en texte, avec colonnes de traçabilité `kafka_*`) |
| Silver | Lignes qui passent les règles qualité (6 tables) |
| Quarantaine | Lignes rejetées, avec une colonne `quarantine_reason` |
| Gold | Modèle en étoile pour l'analyse |

### Gold : étoile Kimball

```mermaid
erDiagram
    DIM_CUSTOMERS ||--o{ FACT_ORDER_ITEMS : customer_key
    DIM_PRODUCTS  ||--o{ FACT_ORDER_ITEMS : product_key
    DIM_SELLERS   ||--o{ FACT_ORDER_ITEMS : seller_key
    DIM_DATE      ||--o{ FACT_ORDER_ITEMS : purchase_date_key
    FACT_ORDER_ITEMS {
        string order_id
        int order_item_id
        long customer_key
        long product_key
        long seller_key
        int purchase_date_key
        string order_status
        double price
        double freight_value
    }
```

- **Grain** : une ligne de `order_items` (un produit, dans une commande, vendu par un vendeur).
- **Mesures** : `price`, `freight_value`.
- **Dimensions dégénérées** : `order_id`, `order_item_id`, `order_status` restent dans la table de faits.
- **`dim_date`** : entièrement dérivée des données, bornes lues dans les dates d'achat réelles.
- **`dim_products`** : enrichie avec la traduction anglaise des catégories.

## Qualité des données

- **Règles uniques** : chaque règle est définie une seule fois (dictionnaire `DATA_QUALITY_RULES` dans `silver_quarantine.py`). La même définition sert à décider si une ligne est valide et à produire la raison de rejet. Le job Silver du flux réutilise ces règles et y ajoute des règles propres au typage. Cette logique est couverte par des tests unitaires (voir [Tests et intégration continue](#tests-et-intégration-continue)).
- **Quarantaine** : une ligne qui viole une ou plusieurs règles va dans `quarantine/<table>/` avec la liste des règles violées dans `quarantine_reason`. Un taux de quarantaine élevé (seuil de 5 % par table) produit une alerte dans les logs, sans faire échouer le job : c'est un problème de **source**, pas de pipeline.
- **Portes bloquantes** : les contrôles d'intégrité du pipeline font échouer la tâche Airflow (code de sortie non nul) : `silver + quarantaine ≠ bronze`, doublon sur `order_id` ou perte de lignes après le `MERGE`, `fact_order_items` différent de `silver/order_items`, clé orpheline, table Bronze vide. Les jobs en aval ne s'exécutent pas. Sur Databricks, la même erreur interrompt le notebook.
- **Validation de la détection** : un test volontaire (règle sur `review_comment_message`) a mis 58 247 lignes en quarantaine, ce qui confirme que le 0 observé avec les règles de base est un vrai négatif, pas un bug.
- **Valeur absente ou mal formée** : dans le flux, les dates restent du texte en Bronze. En Silver, une chaîne vide devient `null` (absence légitime) ; une chaîne non vide illisible déclenche une règle `<colonne>_malformed` distincte, pour que la quarantaine distingue les deux causes.
- **Great Expectations** : utilisé dans `validate_orders.py` (3 expectations sur `orders`). Les règles de Silver sont écrites en PySpark pur, volontairement, pour garder une seule source de vérité ligne par ligne.

### Note sur `order_reviews`

99 224 avis sont chargés en Bronze, contre 104 719 pour `wc -l` sur le CSV. Cet écart n'est pas une perte : certains commentaires clients contiennent des retours à la ligne et des guillemets doublés (`""`), conformes à la norme CSV, et `wc -l` compte alors plusieurs lignes physiques pour un seul enregistrement. Avec `multiLine=true` et `escape='"'`, Spark reconstitue correctement les enregistrements : 0 ligne corrompue détectée, en local comme sur Databricks (même nombre d'avis, 99 224). Un garde-fou reste en place : toute ligne illisible serait conservée dans `quarantine/order_reviews_malformed_csv/` plutôt que perdue.

Les champs de commentaire vides (plus de la moitié des avis) sont une absence de saisie légitime, distincte des lignes mal formées.

## Décisions et compromis

**WSL2 plutôt que Windows natif.** Docker Desktop repose déjà sur WSL2. Développer directement dans WSL2 évite la lenteur d'I/O des montages `/mnt/c/...` et la plupart des problèmes de fins de ligne (qui ont réellement cassé un `.gitignore`). C'est aussi l'environnement le plus proche de la production et des runners Linux de la CI.

**Fins de ligne imposées en LF (`.gitattributes`).** Windows écrit des fins de ligne CRLF, Linux des LF. Un mélange crée de faux changements dans Git, fait échouer des scripts shell dans un conteneur Linux et ferait signaler par `ruff format --check` des écarts de format sur des fichiers non modifiés. `* text=auto eol=lf` normalise tout en LF dans le dépôt ; les fichiers `.jar` sont marqués binaires pour ne jamais être convertis.

**Versions épinglées plutôt que `latest`.** Delta Lake ne fonctionne qu'avec une plage précise de versions de Spark, elle-même liée à une version de Scala. Un décalage ne se voit pas au démarrage mais à l'exécution (`NoSuchMethodError`, `ClassNotFoundException`). Épingler garantit aussi qu'un `docker compose up` relancé dans six mois produit le même environnement. Le même raisonnement vaut pour la CI : `ruff` est épinglé, car une mise à jour pourrait ajouter des règles et casser la CI sans qu'aucun code n'ait changé.

**Fork `pgsty/silo` plutôt que l'image MinIO officielle.** En septembre 2026, MinIO a retiré ses images `minio/minio` de Docker Hub, après être passé en mode maintenance. Deux options : figer une ancienne image officielle, qui ne recevrait plus de correctifs de sécurité, ou utiliser un fork maintenu compatible S3. J'ai retenu le fork. Compromis assumé : un projet plus jeune et moins éprouvé, en échange d'une maintenance de sécurité active.

**Jars non committés.** `aws-java-sdk-bundle` pèse environ 270 Mo, au-delà de la limite de 100 Mo par fichier de GitHub. Il est téléchargeable à l'identique depuis Maven Central. Un premier commit les avait inclus par erreur et le push avait été rejeté : l'historique a été réécrit et les jars sont désormais exclus via `.gitignore` et documentés dans le Setup.

**Image Spark personnalisée via Dockerfile.** Les jars se montent comme volumes, mais une dépendance Python (Great Expectations, boto3, confluent-kafka) doit être installée dans l'environnement du conteneur. Un `pip install` manuel disparaît à la recréation du conteneur : le `Dockerfile` rend l'installation reproductible.

**`PYTHONPATH` plutôt que `--py-files`.** Pour partager `src/common/` entre les scripts, `PYTHONPATH` suffit sur un conteneur unique. Sur Databricks serverless, le même besoin est couvert par le Git folder et l'ajout de `src/` au `sys.path` par un chemin absolu (le répertoire de travail n'étant pas garanti, les imports relatifs sont à éviter). Sur un cluster classique ou en production, il faudrait empaqueter le code (`--py-files`, wheel) : dette technique assumée.

**Grain `order_items` et `customer_id`.** Dans Olist, `customer_id` est unique par commande ; la personne réelle est `customer_unique_id`. La dimension reste au grain `customer_id` (jointure simple) et conserve `customer_unique_id` en attribut. Une analyse fine des clients récurrents demanderait une dimension au grain `customer_unique_id`.

**Clés de substitution par `row_number()`.** Déterministes, contrairement à `monotonically_increasing_id()` qui dépend du partitionnement. Valable parce que chaque dimension est dédoublonnée et réécrite en entier à chaque exécution. Pour de l'incrémental ou du streaming, il faudra une clé hachée stable (par exemple `xxhash64` de la clé naturelle).

**`overwrite` en Bronze batch.** Chaque exécution reconstruit la table à l'identique depuis le CSV (rechargement complet, idempotent). Le flux, lui, écrit en `append` avec un checkpoint.

**Image Kafka officielle `apache/kafka`, mode KRaft.** Pas de ZooKeeper : un seul conteneur joue les rôles de broker et de contrôleur. Deux listeners : `INTERNAL` (`kafka:19092`) pour les conteneurs, `EXTERNAL` (`localhost:9092`) pour la machine hôte. Un client Kafka se reconnecte à l'adresse que le broker lui **annonce** : annoncer `localhost` à un autre conteneur le ferait pointer sur lui-même.

**Clé = `order_id`, 3 partitions.** Kafka ne garantit l'ordre qu'à l'intérieur d'une partition : la clé envoie toutes les occurrences d'une même commande dans la même partition. Vérifié : les 100 mêmes commandes se répartissent à l'identique (28 / 39 / 33) sur deux exécutions indépendantes.

**`confluent-kafka` plutôt que `kafka-python`.** Les deux sont maintenues. `confluent-kafka` s'appuie sur `librdkafka` (client C de référence, plus rapide) et est celle qu'on rencontre le plus en entreprise ; contrepartie : une dépendance native, alors que `kafka-python` est du Python pur.

**Table Bronze du flux séparée (`orders_stream`).** `bronze/orders` est réécrite en `overwrite` par le job batch et son schéma est typé, alors que le flux est en texte brut. Contrepartie : Silver doit consommer les deux.

**Checkpoint dans MinIO et exactement une fois.** Le checkpoint mémorise les offsets Kafka traités ; le sink Delta enregistre dans son journal l'identifiant de la requête et le numéro de micro-batch, ce qui rend l'écriture idempotente si un batch est rejoué après une panne. Vérifié : 100 → 100 (aucun nouveau message, aucun nouveau commit) → 200 lignes, 0 doublon. `trigger(availableNow=True)` vide le topic puis s'arrête : adapté à un déclenchement par un orchestrateur.

**Connecteur Kafka via `--packages`.** C'est la méthode documentée par Spark, qui résout automatiquement les dépendances. Contrepartie : un accès à Maven Central à l'exécution, donc une reproductibilité moindre que les jars montés. À réévaluer plus tard.

**`MERGE` plutôt que `overwrite` pour appliquer le flux à Silver.** Un `overwrite` remplacerait l'historique par les seules lignes du flux. Le `MERGE` sur `order_id` met à jour les commandes existantes et insère les nouvelles, de façon transactionnelle. La source doit être dédupliquée au préalable (un `MERGE` échoue si plusieurs lignes sources visent la même ligne cible). Vérifié : les 200 commandes du flux, déjà présentes dans Silver, sont mises à jour sans insertion ni doublon (99 441 lignes avant et après).

**Airflow en `LocalExecutor`, dans un projet Compose séparé.** Sur une seule machine, le CeleryExecutor demanderait Redis et des workers sans bénéfice. Airflow ne calcule rien ici : il déclenche des jobs dont le calcul se fait dans Spark. Le compose officiel reste proche de l'original ; les choix propres au projet sont isolés dans un fichier de surcharge.

**Lancer Spark depuis Airflow par `docker exec` et le socket Docker.** Un seul environnement Spark à maintenir (jars, versions) plutôt qu'un second dans l'image d'Airflow. Contrepartie de sécurité assumée : le socket Docker donne à qui y accède un contrôle équivalent à `root` sur le moteur. Acceptable sur une machine de développement ; en production, on passerait par un proxy de socket restreint, un `DockerOperator` ou Kubernetes. Le socket n'est monté que dans le scheduler (pas dans le composant exposé sur le port 8080), et l'accès passe par un groupe dédié (`DOCKER_GID`) plutôt que par `root`.

**Un échec bloquant, une quarantaine non bloquante.** Une ligne perdue ou dupliquée, une clé orpheline ou un volume incohérent signalent un pipeline cassé : la tâche échoue et rien ne se publie en aval. Des lignes en quarantaine, elles, montrent que le pipeline a fait son travail face à une source de mauvaise qualité : bloquer tout le pipeline pour cela empêcherait de publier les données valides. Un taux anormal produit seulement une alerte.

**Tentatives automatiques seulement parce que chaque tâche est idempotente.** Rejouer une tâche redonne le même état : `bronze_ingest`, `silver_quarantine` et `gold_star_schema` utilisent `overwrite` ; `stream_to_bronze` s'appuie sur le checkpoint et le sink Delta transactionnel ; `silver_orders_from_stream` utilise un `MERGE` sur clé. Les tentatives aident contre les pannes transitoires, pas contre un bug déterministe, qui échoue à l'identique à chaque essai. Cas réel rencontré : un conteneur `spark` sans les variables d'environnement a fait échouer les quatre tentatives de `bronze_ingest` à l'identique, en cinq secondes et avec un message qui nommait la variable manquante.

**Le producer dans un DAG séparé.** En réalité, la source des données est externe au pipeline. Le rejouer à chaque run enverrait les mêmes commandes en doublon ; il est donc simulé à part, sans tentative automatique (`retries=0`), pour la même raison.

**`max_active_runs=1`.** Delta Lake sur S3 n'est sûr que s'il n'y a qu'un seul écrivain à la fois par table : deux runs simultanés ne doivent pas écrire dans les mêmes tables.

**Une CI en deux jobs séparés, `lint` et `test`.** Ils tournent en parallèle : le lint (quelques secondes) échoue vite sans attendre Spark, et un job rouge indique tout de suite si le problème est de style ou de logique. Un nouveau push annule le run précédent de la même branche (`concurrency`), les permissions du workflow sont limitées à la lecture, et chaque job a un `timeout-minutes` pour qu'un test bloqué ne tourne pas indéfiniment.

**Tests sur la logique, sans infrastructure.** La CI n'a ni MinIO, ni Kafka, ni Airflow. `split_valid_and_quarantine` est une fonction « DataFrame en entrée, DataFrames en sortie », sans lecture ni écriture : elle se teste sur une SparkSession locale (`local[2]`, 2 partitions de shuffle, UI désactivée) en quelques secondes. Tester le pipeline complet serait un test d'intégration, plus lent et plus fragile, qui reste le rôle d'Airflow. Les règles de qualité étant définies une seule fois et réutilisées partout, elles sont testables isolément : c'est la conséquence directe du choix de règles uniques.

**`requirements.txt` et `requirements-dev.txt` séparés.** Le premier liste ce dont le pipeline a besoin pour **tourner** (installé dans l'image Spark). Le second ajoute ce qui sert à **développer** : `pytest`, `ruff`, et `pyspark`. `pyspark` n'a pas sa place dans le premier : Spark est déjà dans l'image Docker, et l'installer en double risquerait un conflit de versions. Le second fichier commence par `-r requirements.txt` pour ne rien dupliquer.

**Java 17 en CI, Java 11 dans le conteneur Spark.** Spark 3.5 supporte Java 8, 11, 17 et 21. Les tests utilisent un pyspark local, pas l'image Docker : la version de Java de la CI n'a donc pas besoin d'être identique à celle du conteneur, et 17 est la version la plus courante en production. La version de `pyspark` des tests (3.5.9) est en revanche la même que celle du cluster, pour tester les règles sur le même moteur.

**Secrets dans l'environnement, sans valeur par défaut.** Un secret écrit dans le code finit dans l'historique Git, où il reste même après suppression. Les secrets sont donc lus depuis des variables d'environnement, avec un `.env.example` qui montre la forme sans les valeurs. Choix de conception : pas de valeur par défaut pour un secret, car un job qui refuse de démarrer est préférable à un job qui tourne avec un mot de passe oublié. Les variables sont lues dans les fonctions et non à l'import, ce qui évite d'exiger des secrets dans la CI. Les clés d'Airflow générées automatiquement (`airflow.cfg`) sont ignorées par Git.

**Une couche d'accès au stockage (`lake.py`) plutôt que des notebooks dupliqués.** Les jobs batch contenaient une dizaine d'endroits qui connaissaient le stockage (chemins `s3a://`, création de buckets, arrêt de la session). Les isoler derrière cinq fonctions (`read_table`, `write_table`, `raw_dir`, `ensure_layers`, `get_spark` / `stop_spark`) permet de n'avoir qu'**une** version du code métier : une règle qualité corrigée l'est partout, et la CI la teste. Recopier le code dans des notebooks aurait créé deux versions qui divergent au premier correctif. Contrepartie : un module de plus, et un comportement à tester pour chaque backend (c'est le rôle de `test_lake.py`). Le backend par défaut reste `local`, pour que le pipeline Airflow ne change pas.

**Tables gérées Unity Catalog et Volumes plutôt que chemins de stockage sur Databricks.** Free Edition ne donne pas accès à un stockage cloud personnel : les couches deviennent des schémas (`workspace.bronze`, `silver`, `gold`, `quarantine`) et les CSV vivent dans un Volume. L'écriture reste un `overwrite`, donc idempotente comme en local ; seul le moyen d'adresser les données change.

**Le streaming Kafka reste en local.** Le calcul serverless n'accepte pas les coordonnées Maven (donc pas le connecteur Kafka de Spark) et ne peut pas joindre un Kafka sur un portable. Plutôt que de dégrader le streaming pour qu'il « passe », seule la partie batch est portée, et la limite est documentée.

## Limites connues

- Les mots de passe de PostgreSQL et le compte administrateur d'Airflow (`airflow` / `airflow`) restent aux valeurs par défaut du compose officiel : acceptable pour un environnement local, à déplacer vers des variables d'environnement. Postgres ne lit son mot de passe qu'à la création de son volume : le changer demande de recréer ce volume.
- Les anciens identifiants de développement de MinIO figurent dans l'historique Git (avant leur déplacement vers `.env`). Ils protègent un conteneur local et ont été remplacés par de nouvelles valeurs ; l'historique n'a pas été réécrit.
- La CI ne teste pas l'intégration : pas de MinIO, Kafka ni Airflow dans le workflow, et aucune connexion à Databricks. Les tests des DAG se limitent à la lecture du code (syntaxe et réglages de sécurité) ; ils ne détectent ni un cycle de dépendances ni un import Airflow cassé, ce qui demanderait d'installer Airflow dans la CI.
- Sur Databricks, seul le batch est porté : `silver.orders` ne contient pas l'upsert du flux. Le notebook se lance à la main (pas de job planifié), et le chemin `repo_src` est saisi manuellement. Les quotas de calcul de Free Edition limitent l'usage et n'ont pas été mesurés.
- Le topic Kafka se crée à la main (étape 6 du Setup) ; le DAG se déclenche manuellement (`schedule=None`).
- `silver_quarantine.py` réécrit `silver/orders` en entier : l'ordre d'exécution compte (batch, puis flux). Une évolution serait de faire du batch aussi un `MERGE`.
- Rejouer le producer sans `--skip` renvoie les mêmes commandes : le checkpoint évite de relire un même message, pas de dédupliquer des messages distincts portant le même `order_id` (c'est le rôle de la déduplication en Silver).
- Delta Lake sur S3 n'est sûr que s'il n'y a qu'un seul écrivain à la fois par table.
- `docker exec` : quand Airflow arrête une tâche (par exemple un dépassement de `execution_timeout`), il interrompt le client `docker exec` mais pas forcément le processus qui tourne dans le conteneur `spark`. Une nouvelle tentative pourrait lancer un second job sur les mêmes tables. Les approches `DockerOperator` ou Kubernetes gèrent mieux le cycle de vie des jobs.
- Gold est validé après avoir été écrit : si une porte échoue, la table défectueuse est déjà publiée. Une amélioration serait de revenir à la version précédente de la table Delta.
- L'alerte de taux de quarantaine n'existe que dans les logs de la tâche : pas de notification externe.
- Deux jobs Spark en parallèle partagent un même conteneur (mémoire, port de l'interface Spark) : correct sur ce volume, à revoir sinon.
- Great Expectations n'est branché que sur `orders` ; les règles de Silver sont en PySpark.
- La fenêtre `row_number()` sans `partitionBy` ramène les données sur une partition : correct à ~100 000 lignes, pas à grande échelle. Databricks le signale d'ailleurs par un avertissement (« No Partition Defined for Window operation ») lors de la construction de Gold.
- Les tests ne mesurent pas leur couverture de code, et la CI ne tourne que sur une version de Python et de Java.
- `boto3` n'est pas épinglé dans `requirements.txt`.

## Métriques

| Mesure | Valeur |
|---|---|
| Fichiers sources | 9 CSV |
| Lignes en Bronze (batch) | environ 1,55 million (dont 1 000 163 pour `geolocation`) |
| Tables Silver avec quarantaine | 6 |
| Dimensions Gold | 4 |
| Lignes de `fact_order_items` | 112 650 (égal à `silver/order_items`) |
| Clés orphelines (4 clés étrangères) | 0 |
| Expectations Great Expectations | 3 (sur `orders`) |
| Taux de quarantaine (règles de base) | 0 % |
| Tests automatisés | 60 (pytest), lint ruff, exécutés à chaque push |
| Plateformes du batch | 2 (local sur MinIO, Databricks Free Edition), même code, mêmes chiffres de contrôle |
| Topic Kafka | 3 partitions, 1 réplique |
| Streaming : lignes ingérées en Bronze | 200 commandes en deux exécutions incrémentales (100 + 100), 0 doublon |
| Upsert du flux dans Silver | 200 lignes mises à jour, 0 insérée, 0 doublon (99 441 lignes avant et après) |
| Streaming : durée d'un micro-batch (100 messages) | environ 1,3 s (premier batch, à froid : non représentatif d'une latence en régime établi) |
| Services Docker Compose | 3 (`minio`, `spark`, `kafka`) + 5 pour Airflow (projet séparé) |

### Durées d'un run complet du DAG `olist_pipeline` (05/11/2026)

Toutes les tâches ont réussi à la première tentative (aucune relance).

| Tâche | Début | Durée |
|---|---|---|
| `bronze_ingest` | 09:13:19 | 1 min 37 |
| `stream_to_bronze` (en parallèle) | 09:13:19 | 56 s |
| `silver_quarantine` | 09:14:57 | 1 min 33 |
| `silver_orders_from_stream` | 09:16:30 | 1 min 04 |
| `gold_star_schema` | 09:17:35 | 1 min 26 |

- **Chemin critique** (`bronze_ingest` → `silver_quarantine` → `silver_orders_from_stream` → `gold_star_schema`) : environ **5 min 40**.
- **Exécution séquentielle équivalente** : environ 6 min 36. Faire tourner la branche streaming en parallèle masque entièrement ses 56 s (environ 14 % de gain).
- Chaque durée inclut le démarrage d'une session Spark par tâche, ce qui explique qu'un job de quelques secondes de calcul dure plus d'une minute.

### Durées du batch sur Databricks (09/11/2026, un seul run)

| Étape | Durée |
|---|---|
| Bronze | environ 1 min (arrondi par l'interface) |
| Silver + quarantaine | 45 s |
| Gold | 24 s |
| Contrôle final | 5 s |

Non comparable au tableau local ci-dessus (session Spark partagée, calcul managé, pas de démarrage de conteneur par tâche).

À mesurer dans les prochaines phases : latence en régime établi, temps de requête avant/après `OPTIMIZE`.

## Avancement

- [x] Environnement Docker Compose (MinIO + Spark + Delta)
- [x] Connexion Spark ↔ MinIO validée
- [x] Bronze : 9 tables Delta
- [x] Silver + table de quarantaine, avec réconciliation des volumes
- [x] Couche Gold : modèle en étoile Kimball
- [x] Kafka (KRaft) + producer Python (clé = `order_id`)
- [x] Spark Structured Streaming : Kafka → `bronze/orders_stream` (checkpoint, exactement une fois)
- [x] Silver alimenté depuis le flux par `MERGE`
- [x] Airflow (`LocalExecutor`) : DAG du pipeline complet, portes qualité bloquantes, tentatives, un seul run actif
- [x] CI GitHub Actions : lint (`ruff`) et 60 tests unitaires (`pytest`)
- [x] Secrets déplacés vers des variables d'environnement (`.env`, `.env.example`)
- [x] Portage du batch sur Databricks Free Edition (couche `lake.py`, notebook `run_batch.py`, chiffres identiques au local)
- [ ] Planification du DAG et alerte externe sur le taux de quarantaine
- [x] Schéma d'architecture final (Mermaid, section [Architecture](#architecture))