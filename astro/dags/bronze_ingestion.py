# from datetime import datetime, timedelta
from datetime import datetime, timedelta
import os
import logging
from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from dotenv import load_dotenv

from include.lib.aws_manager import AWSManager
from include.lib.duckdb_manager import DuckDBManager
from include.lib.motherduck_manager import MotherDuckManager
from include.ingestion.bronze.cards import DataManager


# Load environment variables
load_dotenv()

# Configuration
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

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 4, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

logger = logging.getLogger(__name__)


@dag(default_args=default_args, catchup=False, tags=['bronze_cards'])
def ingestor_bronze():
    """
    Airflow DAG for ingesting data from an API, parsing it, and saving it.
    """

    @task
    def bronze_cards():
        try:
            duckdb_manager = DuckDBManager()
            MotherDuckManager(
                duckdb_manager, MOTHERDUCK_TOKEN
            )
            AWSManager(
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
            
            logger.info("START CREATING TABLE FROM S3")
            data_manager.create_table_from_json_file()
            logger.info("START SAVING DATA LOCALLY")
            data_manager.save_to_local()
            logger.info("START SAVING DATA TO S3")
            data_manager.save_to_s3()
            logger.info("START SAVING DATA TO MD")
            data_manager.save_to_md()
        except Exception as e:
            logger.error(f"An error occured: {e}")

    bronze_cards_task = bronze_cards()
    
    # Define task to trigger the Silver DAG
    trigger_silver_task = TriggerDagRunOperator(
        task_id='trigger_silver',
        trigger_dag_id='ingestor_silver',
        wait_for_completion=True,
        deferrable=True,
    )
        
    # Define tasks dependencies
    bronze_cards_task >> trigger_silver_task


# Instantiate the DAG
ingestor_bronze_dag = ingestor_bronze()
