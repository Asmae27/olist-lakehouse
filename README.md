# Lakehouse Olist en streaming

Projet de portfolio en Data Engineering : reconstruction d'une architecture
lakehouse (medallion) autour du dataset public Olist, avec ingestion batch
puis streaming, contrôles qualité et orchestration.

## Architecture

Producer Python (rejoue les commandes Olist)
→ Kafka
→ Spark Structured Streaming
→ Delta Lake sur MinIO (S3)
Bronze (brut)
→ Silver (nettoyé + contrôles Great Expectations, table de quarantaine)
→ Gold (modèle en étoile Kimball)
→ orchestration Airflow, CI GitHub Actions, le tout en Docker Compose


Portage prévu ensuite vers Databricks Free Edition (partie batch), avec
remplacement optionnel de MinIO par un bucket S3/GCS et test de charge sur
un jeu de données plus volumineux (NYC TLC).

**État actuel** : Bronze en cours de construction (voir section
"Avancement" plus bas). Le streaming Kafka et l'orchestration Airflow ne
sont pas encore implémentés.

## Stack technique

| Composant     | Version                                  |
|---------------|-------------------------------------------|
| Spark         | 3.5.9 (Scala 2.12, Java 11)                |
| Delta Lake    | 3.3.x                                      |
| Hadoop client | 3.3.4                                      |
| hadoop-aws    | 3.3.4                                      |
| aws-java-sdk-bundle | 1.12.262                              |
| MinIO         | fork `pgsty/silo:RELEASE.2026-09-16T00-00-00Z` |

Les versions sont épinglées volontairement (voir section "Décisions" ci-dessous).

## Prérequis

- Docker + Docker Compose
- WSL2 (si Windows) avec au moins ~10 Go de RAM alloués
- Environ 300 Mo d'espace disque pour les connecteurs Spark/S3 (non
  versionnés, voir ci-dessous)

## Setup

1. Cloner le dépôt :
```bash
   git clone https://github.com/Asmae27/olist-lakehouse.git
   cd olist-lakehouse
```

2. Télécharger les connecteurs Spark ↔ S3 (non committés dans Git — voir
   la section "Décisions") :
```bash
   mkdir -p docker/spark/jars
   cd docker/spark/jars
   wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
   wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
   cd ../../..
```

3. Télécharger le dataset Olist ("Brazilian E-Commerce Public Dataset by
   Olist" sur Kaggle) et placer les CSV dans `data/raw/`.

4. Copier `.env.example` vers `.env` et ajuster si besoin.

5. Lancer les services :
```bash
   docker compose up -d
   docker compose ps
```
   Les deux services (`minio`, `spark`) doivent apparaître `healthy` /
   `running`.

6. Console MinIO accessible sur `http://localhost:9001`
   (identifiants dans `.env`).

## Décisions et compromis

**WSL2 plutôt que Windows natif**
Docker Desktop sur Windows repose de toute façon sur WSL2 en interne : les
conteneurs tournent dans un environnement Linux, que le code source soit
édité sous Windows ou non. Développer directement dans WSL2 évite deux
classes de problèmes rencontrés concrètement pendant ce projet : les
fins de ligne CRLF ajoutées par les éditeurs Windows (qui ont fait échouer
un `.gitignore` en cours de route) et la lenteur d'I/O quand Docker monte
des fichiers depuis `/mnt/c/...` plutôt que depuis le système de fichiers
Linux natif. C'est aussi l'environnement le plus proche de la production
et des runners Linux utilisés par GitHub Actions (CI), ce qui limite les
écarts « ça marche chez moi ».

**Versions épinglées plutôt que `latest`**
Delta Lake n'est pas indépendant de Spark : chaque version de Delta ne
supporte qu'une plage précise de versions de Spark, elle-même liée à une
version de Scala (2.12 ou 2.13). Un décalage entre ces versions ne
provoque pas d'erreur au démarrage, mais une erreur au runtime
(`NoSuchMethodError`, `ClassNotFoundException`) au moment où le code
exécute une opération Delta — un bug difficile à relier à sa vraie cause
si on ne sait pas où chercher. Utiliser `latest` rendrait aussi le projet
non reproductible : un `docker compose up` relancé dans six mois pourrait
tirer une image différente de celle testée aujourd'hui. Ce projet fixe
donc Spark 3.5.9 + Delta 3.3.x + Scala 2.12 + hadoop-aws 3.3.4, une
combinaison vérifiée dans la documentation officielle de chaque projet.

**Fork `pgsty/silo` plutôt que l'image MinIO officielle**
En septembre 2026, en plein milieu de la construction de ce projet, MinIO
a retiré ses images `minio/minio` de Docker Hub : l'édition communautaire
était déjà en mode maintenance depuis décembre 2025, et le dépôt GitHub a
depuis été archivé. Deux options s'offraient alors : figer une ancienne
image officielle (qui ne recevrait plus jamais de correctif de sécurité),
ou basculer vers `pgsty/silo`, un fork communautaire qui a repris la
distribution Docker et continue de publier des correctifs. C'est la même
bascule qu'ont opérée des équipes en production confrontées à la même
rupture. Le compromis assumé : ce fork est plus jeune et moins éprouvé
que le MinIO historique, en échange d'une maintenance de sécurité active
— un arbitrage que je préfère à une image figée et non patchable.

**Jars non committés dans Git**
`hadoop-aws` et `aws-java-sdk-bundle` sont des binaires volumineux
(~270 Mo pour le second), publiquement disponibles sur Maven Central et
identiques pour quiconque les retélécharge — ils n'ont donc aucune raison
d'être versionnés. Ce projet en a d'ailleurs fait l'expérience directe :
un premier commit les incluait par erreur, ce qui a fait échouer le push
vers GitHub (limite de 100 Mo par fichier) et a nécessité de réécrire
l'historique local. Ils sont désormais exclus via `.gitignore` et
redocumentés dans la section Setup, avec la commande `wget` exacte pour
les récupérer.

## Avancement

- [x] Environnement Docker Compose (MinIO + Spark)
- [x] Connexion Spark ↔ MinIO validée
- [ ] Job batch Bronze (lecture CSV Olist → écriture Delta)
- [ ] Contrôles qualité Great Expectations + table de quarantaine
- [ ] Couche Gold (modèle en étoile Kimball)
- [ ] Producer Kafka + Spark Structured Streaming
- [ ] Orchestration Airflow
- [ ] CI GitHub Actions
- [ ] Portage Databricks Free Edition

## Métriques

- Dataset : 99 441 commandes (`olist_orders_dataset.csv`), 9 fichiers CSV
  au total dans `data/raw/`
- Services orchestrés via Docker Compose : 2 (`minio`, `spark`)
- Job de test Spark → MinIO : écriture Parquet réussie dans `bronze/test/`
  - Durée totale du job : ~4,87 s
  - Durée du calcul (stage) : ~4,68 s
  - Durée du commit d'écriture : ~635 ms
  - Nombre de partitions en sortie : 6 (correspond au parallélisme par
    défaut de Spark en local, aligné sur les 6 processeurs alloués à
    WSL2)