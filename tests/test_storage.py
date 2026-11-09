
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from common import storage


@pytest.fixture
def mock_s3(monkeypatch):
    """Simule le client S3 sans démarrer MinIO."""
    s3 = MagicMock()

    monkeypatch.setattr(storage.boto3, "client", MagicMock(return_value=s3))
    monkeypatch.setattr(
        storage, "get_minio_endpoint", lambda: "http://localhost:9000"
    )
    monkeypatch.setattr(
        storage,
        "get_env",
        lambda key: {
            "MINIO_ROOT_USER": "minioadmin",
            "MINIO_ROOT_PASSWORD": "minioadmin",
        }[key],
    )

    return s3


def test_ensure_buckets_exist_when_all_buckets_exist(mock_s3):
    mock_s3.list_buckets.return_value = {
        "Buckets": [{"Name": "bronze"}, {"Name": "silver"}]
    }

    storage.ensure_buckets_exist(["bronze", "silver"])

    mock_s3.create_bucket.assert_not_called()


def test_ensure_buckets_exist_creates_missing_bucket(mock_s3):
    mock_s3.list_buckets.return_value = {
        "Buckets": [{"Name": "bronze"}]
    }

    storage.ensure_buckets_exist(["bronze", "silver"])

    mock_s3.create_bucket.assert_called_once_with(Bucket="silver")


def test_ensure_buckets_exist_creates_multiple_missing_buckets(mock_s3):
    mock_s3.list_buckets.return_value = {
        "Buckets": [{"Name": "bronze"}]
    }

    storage.ensure_buckets_exist(["bronze", "silver", "gold"])

    assert mock_s3.create_bucket.call_count == 2
    mock_s3.create_bucket.assert_any_call(Bucket="silver")
    mock_s3.create_bucket.assert_any_call(Bucket="gold")


@pytest.mark.parametrize(
    "error_code",
    ["BucketAlreadyOwnedByYou", "BucketAlreadyExists"],
)
def test_ensure_buckets_exist_ignores_already_created_bucket(
    mock_s3, error_code
):
    mock_s3.list_buckets.return_value = {"Buckets": []}

    mock_s3.create_bucket.side_effect = ClientError(
        {
            "Error": {
                "Code": error_code,
                "Message": "Bucket already exists",
            }
        },
        "CreateBucket",
    )

    # Ne doit pas lever d'exception.
    storage.ensure_buckets_exist(["bronze"])


def test_ensure_buckets_exist_propagates_unexpected_client_error(mock_s3):
    mock_s3.list_buckets.return_value = {"Buckets": []}

    error = ClientError(
        {
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access denied",
            }
        },
        "CreateBucket",
    )
    mock_s3.create_bucket.side_effect = error

    with pytest.raises(ClientError):
        storage.ensure_buckets_exist(["bronze"])


def test_ensure_buckets_exist_configures_s3_client(mock_s3, monkeypatch):
    client_mock = MagicMock(return_value=mock_s3)
    monkeypatch.setattr(storage.boto3, "client", client_mock)

    storage.ensure_buckets_exist([])

    client_mock.assert_called_once()
    args, kwargs = client_mock.call_args

    assert args == ("s3",)
    assert kwargs["endpoint_url"] == "http://localhost:9000"
    assert kwargs["aws_access_key_id"] == "minioadmin"
    assert kwargs["aws_secret_access_key"] == "minioadmin"
    assert kwargs["region_name"] == "us-east-1"
    assert kwargs["config"].signature_version == "s3v4"
