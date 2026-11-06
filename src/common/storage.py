import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


def ensure_buckets_exist(bucket_names):
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
        if name in existing:
            continue
        try:
            s3.create_bucket(Bucket=name)
            print(f"Bucket créé : {name}")
        except ClientError as e:
            # Un autre job l'a créé entre-temps : ce n'est pas une erreur.
            if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise
