from datetime import datetime
import json
import os
import sys
from typing import Any, Dict, List, Optional
import requests
import boto3
from dotenv import load_dotenv
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from models.card import Card


# Load environment variables
load_dotenv()

# Configuration
API_BASE_URL = "https://api.scryfall.com/bulk-data/"
DATASET_NAME = "default_cards"
TABLE_NAME = "cards"
TABLE_PATH = "data/raw/"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


class APIClient:
    """
    Class for interacting with an API.
    """

    def __init__(self, base_url: str, dataset: str) -> None:
        self.base_url = base_url
        self.dataset = dataset

    def fetch_bulk_data(self) -> Optional[requests.Response]:
        """
        Fetches data from the API.

        Args:
            dataset (str): Name of the dataset to fetch.

        Returns:
            dict: Response JSON data.
        """
        try:
            logger.info("Fetching bulk data")
            # Construct the URL for fetching data
            url = f"{self.base_url}{self.dataset}"
            # Send HTTP GET request to fetch data
            response = requests.get(url, timeout=5)
            # Raise an exception for HTTP errors
            response.raise_for_status()
            logger.success(f"Data fetched successfully from {url}")
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data: {e}")
            return None

    def fetch_cards_data(
        self, response: requests.Response
    ) -> Optional[requests.Response]:
        """
        Fetchs data from the bulk data download uri

        Args:
            response (Response): Response object from the API.

        Returns:
            dict: Response JSON data.
        """
        try:
            logger.info("Fetching cards data")
            # Extract relevant data and update timestamp from the API response
            download_uri = response.json()["download_uri"]
            data = requests.get(download_uri)
            update_timestamp = response.json()["updated_at"]
            # Parse the update timestamp to extract the date
            update_date = datetime.strptime(
                update_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z"
            ).date()
            logger.success(
                f"Data fetched successfully from {download_uri} - Update date: {update_date}"
            )
            return data
        except KeyError as e:
            logger.error(f"Failed to extract data from API response: {e}")
            return None
        except ValueError as e:
            logger.error(f"Failed to parse timestamp: {e}")
            return None


class DataParser:
    """
    Class for parsing data.
    """

    @staticmethod
    def parse_cards(data: requests.Response) -> Optional[List[Card]]:
        """
        Parses JSON data into instances of the Card model.

        Args:
            data (dict): JSON data to parse.

        Returns:
            List[Card]: List of parsed Card instances.
        """
        try:
            logger.info("Parsing data")
            cards_data = data.json()
            parsed_cards = [Card(**card) for card in cards_data]
            logger.success("Data parsing successful!")
            return parsed_cards
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None


class DataSaver:
    """
    Class for saving data.
    """

    def __init__(
        self,
        table_path: str,
        table_name: str,
        bucket_name: Optional[str],
        access_key_id: Optional[str],
        secret_access_key: Optional[str],
    ):
        self.table_path = table_path
        self.table_name = table_name
        self.bucket_name = bucket_name
        if bucket_name:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
            )

    def save_local(self, data: List[Dict[str, Any]]) -> None:
        """
        Saves parsed data to a local file.

        Args:
            data (list): List of parsed data.
            table_path (str): Path to store local data.
            table_name (str): Name of the table.

        Returns:
            None
        """
        try:
            logger.info("Saving data locally")
            path = os.path.join(self.table_path, f"{self.table_name}.json")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as file:
                json.dump([item.dict() for item in data], file, indent=4)
            logger.success(f"Data saved locally to {path}")
        except Exception as e:
            logger.error(f"An error occurred while saving data locally: {e}")

    def save_s3(self, data: List[Dict[str, Any]]) -> None:
        """
        Saves parsed data to AWS S3 bucket.

        Args:
            data (list): List of parsed data.
            s3_client: Boto3 S3 client.
            bucket_name (str): Name of the AWS S3 bucket.
            table_path (str): Path to store data in S3.
            table_name (str): Name of the table.

        Returns:
            None
        """
        try:
            logger.info("Saving data to S3 bucket")
            json_bytes = json.dumps([item.dict() for item in data], indent=4).encode(
                "utf-8"
            )
            key = f"{self.table_path}{self.table_name}.json"
            self.s3_client.put_object(Body=json_bytes, Bucket=self.bucket_name, Key=key)
            logger.success(f"Data saved successfully to S3 bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"An error occurred while saving data to S3 bucket: {e}")


class Ingestor:
    """
    Class for ingesting data from an API, parsing it, and saving it.
    """

    def __init__(self, api_client, data_parser, data_saver):
        self.api_client = api_client
        self.data_parser = data_parser
        self.data_saver = data_saver

    def execute(self) -> None:
        """
        Executes the ingestion process.

        Args:
            dataset (str): Name of the dataset to fetch.
        """
        # Fetch data from the API
        bulk_data = self.api_client.fetch_bulk_data()
        cards_data = self.api_client.fetch_cards_data(bulk_data)

        if cards_data:
            # Parse data
            parsed_data = self.data_parser.parse_cards(cards_data)
            if parsed_data:
                # Save data locally
                self.data_saver.save_local(parsed_data)
                # Save data to S3
                self.data_saver.save_s3(parsed_data)


# Create instances of classes
api_client = APIClient(API_BASE_URL, DATASET_NAME)
data_parser = DataParser()
data_saver = DataSaver(
    TABLE_PATH, TABLE_NAME, AWS_BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY
)

# Create an instance of Ingestor and execute the ingestion process
ingestor = Ingestor(api_client, data_parser, data_saver)
ingestor.execute()
