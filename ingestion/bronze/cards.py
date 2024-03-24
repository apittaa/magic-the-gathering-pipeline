import duckdb
import os
import sys
from dotenv import load_dotenv
from loguru import logger
from typing import Any

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from lib import duckdb_manager, motherduck_manager, aws_manager

load_dotenv()

# Load environment variables
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
RAW_S3_PATH = os.getenv("RAW_S3_PATH")
BRONZE_S3_PATH = os.getenv("BRONZE_S3_PATH")
LOCAL_PATH = "data/bronze/"
TABLE_NAME = "cards"
BRONZE_SCHEMA = "bronze"
LOCAL_DATABASE = "memory"
REMOTE_DATABASE = "magic_the_gathering"


class DataManager:
    """
    Manages data operations.
    """

    def __init__(
        self,
        duckdb_manager,
        local_database: str,
        remote_database: str,
        bronze_schema: str,
        table_name: str,
        local_path: str,
        raw_s3_path: str,
        bronze_s3_path: str,
    ):
        """
        Initializes DataManager.

        Args:
            duckdb_manager (DuckDBManager): Instance of DuckDBManager.
        """
        self.duckdb_manager = duckdb_manager
        self.local_database = local_database
        self.remote_database = remote_database
        self.bronze_schema = bronze_schema
        self.table_name = table_name
        self.local_path = local_path
        self.raw_s3_path = raw_s3_path
        self.bronze_s3_path = bronze_s3_path

    def create_table_from_json_file(self) -> None:
        """
        Creates a table from a JSON file stored in S3.

        Returns:
            None
        """
        try:
            logger.info("Creating cards table locally")
            query = f"""
                CREATE OR REPLACE TABLE {self.local_database}.{self.table_name} AS
                WITH ranked_cards AS (
                    SELECT
                        *
                        , ROW_NUMBER() OVER (PARTITION BY id ORDER BY released_at DESC) AS row_num
                    FROM read_json_auto('{self.raw_s3_path}{self.table_name}.json')
                )
                SELECT
                    * EXCLUDE (row_num)
                FROM ranked_cards
                WHERE row_num = 1;
                """
            self.duckdb_manager.execute_query(query)
            logger.success("Cards table created!")
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
            logger.success("Cards table saved locally!")
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
                TO '{self.bronze_s3_path}{self.table_name}.parquet'
                (FORMAT PARQUET)
                """
            self.duckdb_manager.execute_query(query)
            logger.success("Cards table saved to s3!")
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
                f"CREATE DATABASE IF NOT EXISTS {self.remote_database}"
            )
            self.duckdb_manager.execute_query(
                f"CREATE SCHEMA IF NOT EXISTS {self.remote_database}.{self.bronze_schema};"
            )
            query = f"""
                CREATE OR REPLACE TABLE {self.remote_database}.{self.bronze_schema}.{self.table_name} AS
                    SELECT
                        *
                    FROM {self.local_database}.{self.table_name};
                """
            self.duckdb_manager.execute_query(query)
            logger.info("Cards table saved!")
        except Exception as e:
            logger.error(f"Error saving to MotherDuck: {e}")


class Ingestor:
    """
    Orchestrates the entire data ingestion process.
    """

    def __init__(
        self,
        duckdb_manager,
        motherduck_manager,
        aws_manager,
        data_manager,
    ):
        """
        Initializes Ingestor.
        """
        self.duckdb_manager = duckdb_manager
        self.motherduck_manager = motherduck_manager
        self.aws_manager = aws_manager
        self.data_manager = data_manager

    def execute(self) -> None:
        """
        Ingests data by executing the entire data ingestion process.

        Returns:
            None
        """
        try:
            logger.info("Starting ingestion")
            self.data_manager.create_table_from_json_file()
            self.data_manager.save_to_local()
            self.data_manager.save_to_s3()
            self.data_manager.save_to_md()
            logger.success("Ingestion completed!")
        except Exception as e:
            logger.error(f"Error executing data ingestion process: {e}")


if __name__ == "__main__":
    # Create instances of classed
    duckdb_manager = duckdb_manager.DuckDBManager()
    motherduck_manager = motherduck_manager.MotherDuckManager(
        duckdb_manager, MOTHERDUCK_TOKEN
    )
    aws_manager = aws_manager.AWSManager(
        duckdb_manager, AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY
    )
    data_manager = DataManager(
        duckdb_manager,
        LOCAL_DATABASE,
        REMOTE_DATABASE,
        BRONZE_SCHEMA,
        TABLE_NAME,
        LOCAL_PATH,
        RAW_S3_PATH,
        BRONZE_S3_PATH,
    )

    # Creating an instance of DataIngestor and execute the ingestion process
    ingestor = Ingestor(duckdb_manager, motherduck_manager, aws_manager, data_manager)
    ingestor.execute()
