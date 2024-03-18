from datetime import datetime
import json
import os
import requests

import boto3

from dotenv import load_dotenv

from loguru import logger


class Ingestor:
    
    def __init__(self, url, dataset, table_name, table_path, bucket_name, aws_access_key, aws_secret_access_key):
        self.url = url
        self.dataset = dataset
        self.table_name = table_name
        self.table_path = table_path
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=aws_access_key,
                                      aws_secret_access_key=aws_secret_access_key)
        # Configure Loguru to log messages to both the console and a file
        logger.add("app.log", rotation="1 day")  # Log file will rotate daily
        
    def get_data_object(self):
        try:
            # Construct the URL for fetching data
            url = f"{self.url}/{self.dataset}"
            
            # Send HTTP GET request to fetch data
            response = requests.get(url, timeout=5)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            logger.info("Data object fetched successfully from {}", url)
            return response
        
        except requests.exceptions.RequestException as e:
            logger.error("Failed to fetch data object: {}", e)
            return None
    
    def get_data(self, response):
        try:
            # Extract relevant data and update timestamp from the API response
            data = response.json()["download_uri"]
            update_timestamp = response.json()["updated_at"]
            
            # Parse the update timestamp to extract the date
            update_date = datetime.strptime(update_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z").date()
            
            logger.info("Data extracted successfully. Update date: {}", update_date)
            return data, update_date
        
        except KeyError as e:
            logger.error("Failed to extract data from API response: {}", e)
            return None, None
        
        except ValueError as e:
            logger.error("Failed to parse timestamp: {}", e)
            return None, None
    
    def save_data_local(self, data):
        try:
            # Create folder if it does not exist
            if not os.path.exists(os.path.dirname(self.table_path)):
                os.makedirs(os.path.dirname(self.table_path))
            
            # Fetch JSON data from the download URI
            data_response = requests.get(data)
            data_response.raise_for_status()  # Raise an exception for HTTP errors
            
            # Save the JSON data to a local file
            path = f"{self.table_path}{self.table_name}.json"
            json.dump(data_response.json(), open(path, 'w'), indent=4)
            logger.info("Data saved locally to {}", path)
            
        except requests.exceptions.RequestException as e:
            logger.error("Failed to download JSON data: {}", e)
        
        except json.JSONDecodeError as e:
            logger.error("Failed to decode JSON data: {}", e)
        
        except Exception as e:
            logger.error("An unexpected error occurred: {}", e)
            
    def save_data_s3(self, data):
        try:
            # Fetch JSON data from the download URI
            response = requests.get(data)
            response.raise_for_status()  # Raise exception for HTTP errors

            # Convert response JSON to bytes
            json_bytes = json.dumps(response.json(), indent=4).encode('utf-8')

            # Upload JSON data to S3
            self.s3_client.put_object(Body=json_bytes, Bucket=self.bucket_name, Key=f"{self.table_path}{self.table_name}.json")

            logger.info("Data saved successfully to S3 bucket: {}", self.bucket_name)
        except requests.exceptions.RequestException as e:
            logger.error("Failed to fetch data from URL: {}", data)
            logger.error("Error: {}", e)
        except Exception as e:
            logger.error("An error occurred while saving data to S3 bucket:")
            logger.error(e)
        
    def execute(self):
        # Fetch data object from the API
        response = self.get_data_object()
        
        if response is not None:
            # Extract data and update date from the response
            data, update_date = self.get_data(response)
            
            if data is not None:
                # Save data locally
                self.save_data_local(data)
                # Save data s3
                self.save_data_s3(data)
            else:
                logger.error("Failed to fetch data.")
        
        else:
            logger.error("Failed to fetch data object.")


load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

url = "https://api.scryfall.com/bulk-data/"
dataset = "default_cards"  # one entry in db per printed card / "oracle_cards" one entry in db per card, multiple printings of the same card are unified
table_name = "cards"
table_path = "data/raw/"

ingest = Ingestor(url, dataset, table_name, table_path, AWS_BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
ingest.execute()
