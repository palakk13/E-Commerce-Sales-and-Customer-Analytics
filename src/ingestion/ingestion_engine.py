"""
Modular Data Ingestion Engine.

Ingests raw operational datasets (CSV/JSON), computes cryptographic SHA-256
checksums, audits file sizes, timestamps, and row counts, ensuring immutable,
read-only handling of raw data sources.
"""

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from config.logging_config import setup_logger
from config.pipeline_config import config

logger = setup_logger("ingestion_engine")


@dataclass
class IngestionAuditRecord:
    """Audit metadata recorded for each ingested dataset."""
    entity_name: str
    file_path: str
    file_hash_sha256: str
    file_size_bytes: int
    row_count: int
    column_count: int
    columns: List[str]
    ingestion_status: str  # "SUCCESS" or "FAILED"
    ingested_at: str
    duration_ms: float
    error_message: Optional[str] = None


class DataIngestionEngine:
    """
    Modular ingestion manager for operational e-commerce data files.
    Preserves raw files as strictly immutable read-only assets.
    """

    def __init__(self, raw_dir: Optional[Path] = None):
        self.raw_dir = Path(raw_dir) if raw_dir else config.paths.raw_dir
        self.audit_records: Dict[str, IngestionAuditRecord] = {}

    @staticmethod
    def compute_sha256(file_path: Path) -> str:
        """Computes the SHA-256 cryptographic hash of a file for data lineage and immutability audits."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def ingest_file(self, file_path: Path, entity_name: str) -> Tuple[pd.DataFrame, IngestionAuditRecord]:
        """
        Reads a raw file into a pandas DataFrame and generates an audit record.

        Args:
            file_path: Path to the raw source file.
            entity_name: Logical entity identifier (e.g. 'customers', 'orders').

        Returns:
            Tuple of (pd.DataFrame, IngestionAuditRecord).
        """
        start_time = time.perf_counter()
        utc_timestamp = datetime.now(timezone.utc).isoformat()

        logger.info("Ingesting raw file: %s for entity: '%s'...", file_path.name, entity_name)

        if not file_path.exists():
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            error_msg = f"Source file does not exist: {file_path}"
            logger.error(error_msg)
            audit = IngestionAuditRecord(
                entity_name=entity_name,
                file_path=str(file_path),
                file_hash_sha256="",
                file_size_bytes=0,
                row_count=0,
                column_count=0,
                columns=[],
                ingestion_status="FAILED",
                ingested_at=utc_timestamp,
                duration_ms=round(duration_ms, 2),
                error_message=error_msg,
            )
            self.audit_records[entity_name] = audit
            raise FileNotFoundError(error_msg)

        try:
            # 1. Compute file metadata and cryptographic hash
            file_size = file_path.stat().st_size
            file_hash = self.compute_sha256(file_path)

            # 2. Read file (all raw string/numeric values preserved as read)
            # keep_default_na=False helps distinguish between raw empty strings and missing fields
            df = pd.read_csv(file_path, dtype=str, keep_default_na=False)

            duration_ms = (time.perf_counter() - start_time) * 1000.0

            audit = IngestionAuditRecord(
                entity_name=entity_name,
                file_path=str(file_path),
                file_hash_sha256=file_hash,
                file_size_bytes=file_size,
                row_count=len(df),
                column_count=len(df.columns),
                columns=list(df.columns),
                ingestion_status="SUCCESS",
                ingested_at=utc_timestamp,
                duration_ms=round(duration_ms, 2),
                error_message=None,
            )

            self.audit_records[entity_name] = audit
            logger.info(
                "Successfully ingested '%s': %d rows, %d columns, hash=%s (%0.2f ms)",
                entity_name,
                audit.row_count,
                audit.column_count,
                file_hash[:12] + "...",
                duration_ms,
            )
            return df, audit

        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            error_msg = f"Failed to ingest {file_path}: {str(e)}"
            logger.exception(error_msg)
            audit = IngestionAuditRecord(
                entity_name=entity_name,
                file_path=str(file_path),
                file_hash_sha256="",
                file_size_bytes=0,
                row_count=0,
                column_count=0,
                columns=[],
                ingestion_status="FAILED",
                ingested_at=utc_timestamp,
                duration_ms=round(duration_ms, 2),
                error_message=error_msg,
            )
            self.audit_records[entity_name] = audit
            raise

    def ingest_all(self) -> Dict[str, Tuple[pd.DataFrame, IngestionAuditRecord]]:
        """
        Executes ingestion for all core e-commerce operational entities.

        Returns:
            Dictionary mapping entity names to (DataFrame, IngestionAuditRecord) tuples.
        """
        logger.info("=== Starting Ingestion Pipeline for Raw Entities ===")
        results = {}

        entity_file_mapping = {
            "customers": self.raw_dir / config.paths.customers_file,
            "products": self.raw_dir / config.paths.products_file,
            "orders": self.raw_dir / config.paths.orders_file,
            "order_items": self.raw_dir / config.paths.order_items_file,
        }

        for entity, fpath in entity_file_mapping.items():
            df, audit = self.ingest_file(fpath, entity)
            results[entity] = (df, audit)

        # Save audit log manifest to logs directory
        self.save_audit_manifest()
        return results

    def save_audit_manifest(self, output_dir: Optional[Path] = None) -> Path:
        """Persists the ingestion audit manifest to a JSON file."""
        target_dir = Path(output_dir) if output_dir else config.paths.logs_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = target_dir / "ingestion_manifest.json"

        manifest_data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "entities": {k: asdict(v) for k, v in self.audit_records.items()},
        }

        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2)

        logger.info("Ingestion audit manifest saved to: %s", manifest_path.resolve())
        return manifest_path


if __name__ == "__main__":
    engine = DataIngestionEngine()
    ingested = engine.ingest_all()
    print("\n--- Ingestion Summary ---")
    for entity, (df, audit) in ingested.items():
        print(f"[{entity.upper()}] Rows: {audit.row_count} | SHA256: {audit.file_hash_sha256[:12]}... | Time: {audit.duration_ms}ms")
