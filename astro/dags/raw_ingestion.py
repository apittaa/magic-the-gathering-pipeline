# from datetime import datetime, timedelta
from datetime import datetime, timedelta
import os
import logging
from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from dotenv import load_dotenv

from include.ingestion.raw.cards import (
    APIClient,
    DataParser,
    DataSaver,
    )

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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

logger = logging.getLogger(__name__)


@dag(default_args=default_args, schedule_interval='@daily', catchup=False, tags=['raw_cards'])
def ingestor_raw():
    """
    Airflow DAG for ingesting data from an API, parsing it, and saving it.
    """

    @task
    def raw_cards():
        try:
            api_client = APIClient(API_BASE_URL, DATASET_NAME)
            data_parser = DataParser()
            data_saver = DataSaver(
                TABLE_PATH, TABLE_NAME, AWS_BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY
            )
            
            logger.info("START FETCHING BULKING DATA")
            bulk_data = api_client.fetch_bulk_data()
            logger.info("START FETCHING CARDS DATA")
            cards_data = api_client.fetch_cards_data(bulk_data)
            logger.info("START PARSING DATA")
            parsed_data = data_parser.parse_cards(cards_data)
            logger.info("START SAVING DATA LOCALLY")
            data_saver.save_local(parsed_data)
            logger.info("START SAVING DATA TO S3")
            data_saver.save_s3(parsed_data)
        except Exception as e:
            logger.error(f"An error occured: {e}")

    # Define task dependencies
    raw_cards_task = raw_cards()
    
    # Define task to trigger the Silver DAG
    trigger_bronze_task = TriggerDagRunOperator(
        task_id='trigger_bronze',
        trigger_dag_id='ingestor_bronze',
        wait_for_completion=True,
        deferrable=True,
    )
        
    # Define tasks dependencies
    raw_cards_task >> trigger_bronze_task


# Instantiate the DAG
ingestor_raw()
