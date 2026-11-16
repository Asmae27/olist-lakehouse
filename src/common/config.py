"""Lecture de la configuration sensible depuis les variables d'environnement."""

import os


def get_env(name: str) -> str:
    """Retourne la variable d'environnement `name`, ou échoue tout de suite avec un message clair.

    Pas de valeur par défaut pour un secret : mieux vaut un job qui refuse de démarrer
    qu'un job qui tourne avec un mot de passe oublié dans le code.
    """
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Variable d'environnement manquante : {name} (voir .env.example)")
    return value


def get_minio_endpoint() -> str:
    """Adresse de MinIO : pas un secret, donc une valeur par défaut est acceptable."""
    return os.environ.get("MINIO_ENDPOINT", "http://minio:9000")


def get_source_db_settings() -> dict:
    """Paramètres de connexion à PostgreSQL, la base « source » dont on capte les changements.

    Hôte, port et nom de base ne sont pas des secrets : ils gardent un défaut. L'utilisateur
    et le mot de passe n'en ont pas (voir `get_env`). Les clés du dictionnaire sont celles
    attendues par `psycopg2.connect(**settings)`.
    """
    return {
        "host": os.environ.get("SOURCE_DB_HOST", "source-db"),
        "port": int(os.environ.get("SOURCE_DB_PORT", "5432")),
        "dbname": os.environ.get("SOURCE_DB_NAME", "olist_source"),
        "user": get_env("SOURCE_DB_USER"),
        "password": get_env("SOURCE_DB_PASSWORD"),
    }
