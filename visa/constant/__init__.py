import os
from datetime import datetime

def get_current_time_stamp():
    return f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"

CURRENT_TIME_STAMP = get_current_time_stamp()
ROOT_DIR = os.getcwd()
CONFIG_DIR = "config"
CONFIG_FILE_NAME = "config.yaml"

CONFIG_FILE_PATH = os.path.join(ROOT_DIR,CONFIG_DIR,CONFIG_FILE_NAME)

### Data Ingested realted variable

DATA_INGESTION_CONFIG_KEY = "data_ingestion_config"
DATA_INGESTION_DOWNLOAD_URL_KEY = 'dataset_download_url'
DATA_INGESTION_ARTIFACT_DIR = "data_ingestion"
DATA_INGESTION_RAW_DATA_DIR_KEY = 'raw_data_dir'
DATA_INGESTION_INGESTED_DATA_DIR_KEY = 'ingested_dir'
DATA_INGESTION_INGESTED_TRAIN_DATA_DIR_KEY = 'ingested_train_dir'
DATA_INGESTION_INGESTED_TEST_DATA_DIR_KEY = 'ingested_test_dir'

### Training pipeline related variable
TRAINING_PIPELINE_CONFIGURATION_KEY = 'training_pipeline_config'
TRAINING_PIPELINE_ARTIFACT_DIR_KEY = 'artifact_dir'
TRAINING_PIPELINE_NAME_KEY = 'pipeline'

## company variable
COLUMN_COMPANY_AGE = 'company_age'
COLUMNS_YEAR_ESTB = 'yr_of_estab'
COLUMN_ID = "case_id"


## Data Validation related variable

## Data transformation Variable

## Model Training variable

## Model Validation

## Model pusher