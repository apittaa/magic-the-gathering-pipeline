from lib.duckdb_manager import DuckDBManager
from loguru import logger


class MotherDuckManager:
    """
    Manages connection to MotherDuck.
    """

    def __init__(self, duckdb_manager: DuckDBManager, motherduck_token: str):
        """
        Initializes MotherDuckManager.

        Args:
            duckdb_manager (DuckDBManager): Instance of DuckDBManager.
            motherduck_token (str): Token for accessing MotherDuck.
        """
        self.duckdb_manager = duckdb_manager
        self.connect(motherduck_token)

    def connect(self, motherduck_token: str) -> None:
        """
        Connects to MotherDuck.

        Args:
            motherduck_token (str): Token for accessing MotherDuck.

        Returns:
            None
        """
        try:
            logger.info("Connecting to Mother Duck")
            self.duckdb_manager.execute_query("INSTALL md;")
            self.duckdb_manager.execute_query("LOAD md;")
            self.duckdb_manager.execute_query(
                f"SET motherduck_token='{motherduck_token}'"
            )
            self.duckdb_manager.execute_query("ATTACH 'md:'")
            logger.success("Connected to Mother Duck!")
        except Exception as e:
            logger.error(f"Error connecting to MotherDuck: {e}")
