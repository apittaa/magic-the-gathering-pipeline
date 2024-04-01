import os
import logging

logger = logging.getLogger(__name__)


class DataManager:
    """
    Manages data operations.
    """

    def __init__(
        self,
        duckdb_manager,
        local_database: str,
        remote_database: str,
        silver_schema: str,
        table_name: str,
        local_path: str,
        bronze_s3_path: str,
        silver_s3_path: str,
    ):
        """
        Initializes DataManager.

        Args:
            duckdb_manager (DuckDBManager): Instance of DuckDBManager.
        """
        self.duckdb_manager = duckdb_manager
        self.local_database = local_database
        self.remote_database = remote_database
        self.silver_schema = silver_schema
        self.table_name = table_name
        self.local_path = local_path
        self.bronze_s3_path = bronze_s3_path
        self.silver_s3_path = silver_s3_path

    def create_table_from_bronze(self) -> None:
        """
        Creates a table from a JSON file stored in S3.

        Returns:
            None
        """
        try:
            logger.info("Creating cards table locally")
            query = f"""
                CREATE OR REPLACE TABLE {self.local_database}.{self.table_name} AS
                SELECT
                    id AS card_id
                    , name
                    , released_at
                    , color_identity
                    , set_name
                    , artist
                    , (prices).usd AS usd_price
                FROM read_parquet('{self.bronze_s3_path}{self.table_name}.parquet');
                """
            self.duckdb_manager.execute_query(query)
            logger.info("Cards table created!")
        except Exception as e:
            logger.error(f"Error creating table from JSON file: {e}")

    def save_to_local(self) -> None:
        """
        Saves data to local disk.

        Returns:
            None
        """
        try:
            logger.info("Saving cards table as parquet format locally")
            os.makedirs(os.path.dirname(self.local_path), exist_ok=True)
            query = f"""
                COPY (
                    SELECT
                        *
                    FROM {self.local_database}.{self.table_name}
                )
                TO '{self.local_path}{self.table_name}.parquet'
                (FORMAT PARQUET)
                """
            self.duckdb_manager.execute_query(query)
            logger.info("Cards table saved locally!")
        except Exception as e:
            logger.error(f"Error saving to local: {e}")

    def save_to_s3(self) -> None:
        """
        Saves data to Amazon S3.

        Returns:
            None
        """
        try:
            logger.info("Saving cards table to s3 as parquet")
            query = f"""
                COPY (
                    SELECT
                        *
                    FROM {self.local_database}.{self.table_name}
                )
                TO '{self.silver_s3_path}{self.table_name}.parquet'
                (FORMAT PARQUET)
                """
            self.duckdb_manager.execute_query(query)
            logger.info("Cards table saved to s3!")
        except Exception as e:
            logger.error(f"Error saving to S3: {e}")

    def save_to_md(self) -> None:
        """
        Saves data to MotherDuck.

        Returns:
            None
        """
        try:
            logger.info("Saving cards table to Mother Duck")
            self.duckdb_manager.execute_query(
                f"CREATE SCHEMA IF NOT EXISTS {self.remote_database}.{self.silver_schema};"
            )
            query = f"""
                CREATE OR REPLACE TABLE {self.remote_database}.{self.silver_schema}.{self.table_name} AS
                    SELECT
                        *
                    FROM {self.local_database}.{self.table_name};
                """
            self.duckdb_manager.execute_query(query)
            logger.info("Cards table saved!")
        except Exception as e:
            logger.error(f"Error saving to MotherDuck: {e}")
