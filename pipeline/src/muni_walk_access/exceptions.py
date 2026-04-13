"""Pipeline-specific exception hierarchy."""

from __future__ import annotations


class PipelineError(Exception):
    """Base exception for pipeline errors."""


class IngestError(PipelineError):
    """Raised when data ingestion fails without cache fallback."""

    def __init__(self, dataset_id: str, message: str) -> None:
        """Initialise with the failing dataset_id and a human-readable message."""
        self.dataset_id = dataset_id
        super().__init__(f"Ingest failed for {dataset_id}: {message}")
