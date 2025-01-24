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
)
import os
from typing import Iterator, Dict, Any
from psycopg2.extras import execute_values
import json

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

        # Define table metadata
        self.metadata = MetaData(schema="raw_340b")
        self.covered_entities = Table(
            "covered_entities",
            self.metadata,
            Column("_loaded_at", TIMESTAMP(timezone=True)),
            Column("ce_id", Integer),
            Column("id_340b", String),
            Column("data", JSON),
            extend_existing=True,
        )

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
            # Get raw connection for using execute_values
            raw_conn = self.engine.raw_connection()
            with raw_conn.cursor() as cur:
                batch = []
                insert_query = """
                    INSERT INTO raw_340b.covered_entities (ce_id, id_340b, data, _loaded_at)
                    VALUES %s
                    ON CONFLICT (ce_id, _loaded_at) DO UPDATE 
                    SET id_340b = EXCLUDED.id_340b,
                        data = EXCLUDED.data;
                """

                for entity in self.stream_entities():
                    # Convert entity to a tuple for execute_values
                    batch.append(
                        (
                            entity.get("ceId"),
                            entity.get("id340B"),
                            json.dumps(entity),
                            datetime.now(),
                        )
                    )

                    if len(batch) >= batch_size:
                        execute_values(cur, insert_query, batch)
                        records_processed += len(batch)
                        batch = []
                        raw_conn.commit()
                        logger.info(f"Processed {records_processed} records")

                # Insert any remaining records
                if batch:
                    execute_values(cur, insert_query, batch)
                    records_processed += len(batch)
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
