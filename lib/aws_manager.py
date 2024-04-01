from lib.duckdb_manager import DuckDBManager
from loguru import logger


class AWSManager:
    """
    Manages AWS credentials and operations.
    """

    def __init__(
        self,
        duckdb_manager: DuckDBManager,
        aws_region: str,
        aws_access_key: str,
        aws_secret_access_key: str,
    ):
        """
        Initializes AWSManager.

        Args:
            duckdb_manager (DuckDBManager): Instance of DuckDBManager.
            aws_region (str): AWS region.
            aws_access_key (str): AWS access key ID.
            aws_secret_access_key (str): AWS secret access key.
        """
        self.duckdb_manager = duckdb_manager
        self.load_credentials(aws_region, aws_access_key, aws_secret_access_key)

    def load_credentials(
        self, aws_region: str, aws_access_key: str, aws_secret_access_key: str
    ) -> None:
        """
        Loads AWS credentials.

        Args:
            aws_region (str): AWS region.
            aws_access_key (str): AWS access key ID.
            aws_secret_access_key (str): AWS secret access key.

        Returns:
            None
        """
        try:
            logger.info("Loading AWS credentials")
            self.duckdb_manager.execute_query("INSTALL httpfs;")
            self.duckdb_manager.execute_query("LOAD httpfs;")
            self.duckdb_manager.execute_query(f"SET s3_region='{aws_region}'")
            self.duckdb_manager.execute_query(
                f"SET s3_access_key_id='{aws_access_key}';"
            )
            self.duckdb_manager.execute_query(
                f"SET s3_secret_access_key='{aws_secret_access_key}';"
            )
            self.duckdb_manager.execute_query("CALL load_aws_credentials();")
            logger.success("AWS credentials loaded!")
        except Exception as e:
            logger.error(f"Error loading AWS credentials: {e}")
