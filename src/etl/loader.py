"""
Raw OPAIS Data Loader
Streams JSON data into raw PostgreSQL tables for further processing with dbt.
"""

import ijson
import logging
from datetime import datetime
from pathlib import Path
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    JSON,
    TIMESTAMP,
    text,
    PrimaryKeyConstraint,
)
import os
from typing import Iterator, Dict, Any
from psycopg2.extras import execute_values
import json
from hashlib import sha256

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class RawDataLoader:
    def __init__(self, database_url: str):
        """Initialize the data loader."""
        self.engine = create_engine(database_url)
        self.data_path = Path("data/raw/OPA_CE_DAILY_PUBLIC.JSON")

        # Define table metadata to match init.sql
        self.metadata = MetaData(schema="raw_340b")
        self.covered_entities = Table(
            "covered_entities",
            self.metadata,
            Column(
                "_loaded_at",
                TIMESTAMP(timezone=True),
                server_default=text("CURRENT_TIMESTAMP"),
            ),
            Column("ce_id", Integer, primary_key=True, unique=True),
            Column("id_340b", String),
            Column("data", JSON),
            Column("data_hash", String(64)),
            extend_existing=True,
        )

    def calculate_hash(self, data: dict) -> str:
        """Calculate a hash of the JSON data in a consistent manner."""
        # Sort keys to ensure consistent ordering
        canonical_json = json.dumps(data, sort_keys=True)
        return sha256(canonical_json.encode()).hexdigest()

    def stream_entities(self) -> Iterator[Dict[str, Any]]:
        """Stream entities from the JSON file."""
        with open(self.data_path, "rb") as file:
            entities = ijson.items(file, "coveredEntities.item")
            for entity in entities:
                yield entity

    def start_load(self) -> int:
        """Start a new load process and return the load_id."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    INSERT INTO raw_340b.load_audit (status)
                    VALUES ('IN_PROGRESS')
                    RETURNING load_id;
                """
                )
            )
            load_id = result.scalar_one()
            conn.commit()
            return load_id

    def finish_load(self, load_id: int, records_processed: int, error: str = None):
        """Mark a load as completed."""
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                    UPDATE raw_340b.load_audit
                    SET end_time = CURRENT_TIMESTAMP,
                        records_processed = :records,
                        status = :status,
                        error_message = :error
                    WHERE load_id = :load_id;
                """
                ),
                {
                    "load_id": load_id,
                    "records": records_processed,
                    "status": "ERROR" if error else "COMPLETED",
                    "error": error,
                },
            )
            conn.commit()

    def load_data(self, batch_size: int = 1000):
        """Load data in batches using execute_values with ON CONFLICT handling."""
        load_id = self.start_load()
        records_processed = 0

        try:
            raw_conn = self.engine.raw_connection()
            with raw_conn.cursor() as cur:
                batch = []
                seen_ce_ids = set()  # Track ce_ids in current batch

                insert_query = """
                    INSERT INTO raw_340b.covered_entities 
                        (ce_id, id_340b, data, data_hash)
                    VALUES %s
                    ON CONFLICT (ce_id) DO UPDATE 
                    SET id_340b = EXCLUDED.id_340b,
                        data = EXCLUDED.data,
                        data_hash = EXCLUDED.data_hash,
                        _loaded_at = CURRENT_TIMESTAMP
                    WHERE raw_340b.covered_entities.data_hash != EXCLUDED.data_hash;
                """

                for entity in self.stream_entities():
                    ce_id = entity.get("ceId")

                    # Skip if we've already seen this ce_id in current batch
                    if ce_id in seen_ce_ids:
                        continue

                    seen_ce_ids.add(ce_id)
                    data_hash = self.calculate_hash(entity)
                    batch.append(
                        (
                            ce_id,
                            entity.get("id340B"),
                            json.dumps(entity),
                            data_hash,
                        )
                    )

                    if len(batch) >= batch_size:
                        execute_values(cur, insert_query, batch)
                        records_processed += len(batch)
                        logging.info(f"Processed {records_processed} records")
                        batch = []
                        seen_ce_ids.clear()  # Clear the set for the next batch

                # Insert any remaining records
                if batch:
                    execute_values(cur, insert_query, batch)
                    records_processed += len(batch)
                    logging.info(f"Processed {records_processed} records")

                raw_conn.commit()

                self.finish_load(load_id, records_processed)
                logger.info(f"Successfully loaded {records_processed} records")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error loading data: {error_msg}")
            self.finish_load(load_id, records_processed, error_msg)
            raise
        finally:
            raw_conn.close()


def main():
    """Main entry point."""
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/opais_340b"
    )
    loader = RawDataLoader(database_url)

    try:
        loader.load_data()
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise


if __name__ == "__main__":
    main()
