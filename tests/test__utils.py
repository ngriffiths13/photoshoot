"""Tests for utils.py"""

import pytest

from photoshoot.utils import InvalidGcsUriError, validate_gcs_uri


def test__validate_gcs_uri__valid():
    validate_gcs_uri("gs://my_bucket/my_file")


def test__validate_gcs_uri__invalid():
    with pytest.raises(InvalidGcsUriError, match="Invalid GCS URI provided. Must start with 'gs://'"):
        validate_gcs_uri("my_bucket/my_file")
