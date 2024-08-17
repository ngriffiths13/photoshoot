"""Utility functions for the project."""


class InvalidGcsUriError(Exception):
    """Exception raised when an invalid GCS URI is provided."""

    pass


def validate_gcs_uri(gcs_uri: str) -> None:
    """Validate a GCS URI.

    Args:
    ----
        gcs_uri (str): The GCS URI to validate.

    Raises:
    ------
        InvalidGcsUriError: If the GCS URI is invalid.

    """
    if not gcs_uri.startswith("gs://"):
        raise InvalidGcsUriError("Invalid GCS URI provided. Must start with 'gs://'")
