import duckdb
from typing import Any
import logging


logger = logging.getLogger(__name__)


class DuckDBManager:
    """
    Manages DuckDB connection and executes queries.
    """

    def __init__(self):
        """
        Initializes DuckDBManager.
        """
        self.connection = self.create_connection()

    def create_connection(self) -> Any:
        """
        Create a connection to DuckDB.

        Returns:
            duckdb.Connection: DuckDB connection object.
        """
        try:
            logger.info("Creating DuckDB connection")
            duckdb_conn = duckdb.connect()
            logger.info("DuckDB connection created!")
            return duckdb_conn
        except Exception as e:
            logger.error(f"Error creating DuckDB connection: {e}")
            return None

    def execute_query(self, query: str) -> None:
        """
        Executes a SQL query.

        Args:
            query (str): SQL query to execute.

        Returns:
            None
        """
        try:
            logger.info("Executing query")
            self.connection.execute(query)
            logger.info("Query executed")
        except Exception as e:
            logger.error(f"Error executing query: {e}")
