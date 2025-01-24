"""
Raw OPAIS Data Loader
Streams JSON data into raw PostgreSQL tables for further processing with dbt.
"""

import ijson
import logging
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text
import os
from typing import Iterator, Dict, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class RawDataLoader:
    def __init__(self, database_url: str):
        """Initialize the data loader."""
        self.engine = create_engine(database_url)
        self.data_path = Path("/app/data/raw/OPA_CE_DAILY_PUBLIC.JSON")

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
        """Load data in batches."""
        load_id = self.start_load()
        records_processed = 0

        try:
            with self.engine.connect() as conn:
                batch = []

                for entity in self.stream_entities():
                    batch.append(
                        {
                            "ce_id": entity.get("ceId"),
                            "id_340b": entity.get("id340B"),
                            "data": entity,
                        }
                    )

                    if len(batch) >= batch_size:
                        conn.execute(
                            text(
                                """
                                INSERT INTO raw_340b.covered_entities 
                                (ce_id, id_340b, data)
                                VALUES (:ce_id, :id_340b, :data::jsonb)
                            """
                            ),
                            batch,
                        )
                        records_processed += len(batch)
                        batch = []
                        conn.commit()
                        logger.info(f"Processed {records_processed} records")

                # Insert any remaining records
                if batch:
                    conn.execute(
                        text(
                            """
                            INSERT INTO raw_340b.covered_entities 
                            (ce_id, id_340b, data)
                            VALUES (:ce_id, :id_340b, :data::jsonb)
                        """
                        ),
                        batch,
                    )
                    records_processed += len(batch)
                    conn.commit()

            self.finish_load(load_id, records_processed)
            logger.info(f"Successfully loaded {records_processed} records")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error loading data: {error_msg}")
            self.finish_load(load_id, records_processed, error_msg)
            raise


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
