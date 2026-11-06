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
