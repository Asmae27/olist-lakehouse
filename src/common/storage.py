import boto3
from botocore.client import Config


def ensure_buckets_exist(bucket_names):
    """Crée les buckets MinIO manquants (S3 exige qu'un bucket existe avant écriture)."""
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    for name in bucket_names:
        if name not in existing:
            s3.create_bucket(Bucket=name)
            print(f"Bucket créé : {name}")