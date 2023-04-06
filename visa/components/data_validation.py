import os
import sys
from visa.constant import *
from visa.loger import logging
from visa.entity.config_entity import DataValidationConfig
from visa.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from visa.config.configuration import Configuartion
from visa.exception import CustomException
from visa.utils.utils import read_yaml_file
from visa.entity.raw_data_validation import IngestedDataValidation

class DataValidation:
    def __init__(self,data_validation_config:DataValidationConfig,data_ingestion_artifact:DataIngestionArtifact):
        try:
            logging.info(f"{'>>>'*30} Data Validation log started {'>>>'*30}")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.schema_path = self.data_validation_config.schema_file_path
            self.train_data = IngestedDataValidation(validate_path=self.data_ingestion_artifact.train_file_path,schema_path=self.schema_path)
            self.test_data = IngestedDataValidation(validate_path=self.data_ingestion_artifact.test_file_path,schema_path=self.schema_path)
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def isFolderPathAvailabel(self)->bool:
        try:
            ## Truue means availabel false means not availabel
            isfolder_availabel = False
            train_path = self.data_ingestion_artifact.train_file_path
            test_path = self.data_ingestion_artifact.test_file_path
            if os.path.exists(train_path):
                if os.path.exists(test_path):
                    isfolder_availabel = True
            return isfolder_availabel
        except Exception as e:
            raise CustomException(e,sys) from e
    
    
    def is_Validation_successful(self)->bool:
        try:
            validation_status = True
            if self.isFolderPathAvailabel == True:
                train_filename = os.path.basename(self.data_ingestion_artifact.train_file_path)
                is_train_filename_validated = self.train_data.validate_filename(filename=train_filename)
                is_train_column_numbers_validated = self.train_data.validate_column_length()
                is_train_column_name_same = self.train_data.check_columns_name()
                is_train_missing_values_whole_column = self.train_data.missing_values_whole_columns()
                self.train_data.replace_null_values_with_null()

                test_filename = os.path.basename(self.data_ingestion_artifact.test_file_path)
                is_test_filename_validated = self.test_data.validate_filename(filename=test_filename)
                is_test_column_numbers_validated = self.test_data.validate_column_length()
                is_test_column_name_same = self.test_data.check_columns_name()
                is_test_missing_values_whole_column = self.test_data.missing_values_whole_columns()
                self.test_data.replace_null_values_with_null()

                if is_train_filename_validated & is_train_column_numbers_validated & is_train_column_name_same & is_train_missing_values_whole_column:
                    pass
                else:
                    validation_status = False
                    logging.info("Check yout Training Data! Validation Failed")
                    raise ValueError("Check your Training data! Validation failed")
                
                if is_test_filename_validated & is_test_column_numbers_validated & is_test_column_name_same & is_test_missing_values_whole_column:
                    pass
                else:
                    validation_status = False
                    logging.info("Check yout Test Data! Validation Failed")
                    raise ValueError("Check your Test data! Validation failed")
                logging.info("validation process completed")
                return validation_status

        except Exception as e:
            raise CustomException(e,sys) from e
        
    def initiate_data_validation(self):
        try:
            data_validation_artifact = DataValidationArtifact(schema_file_path=self.schema_path,is_validated = self.is_Validation_successful(),message = "Data Validation Perfiormed")
            logging.info(f"Data Validation Artifact:{data_validation_artifact}")
            return data_validation_artifact
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def __del__(self):
        logging.info(f"{'>>' * 30}Data Validation log completed.{'<<' * 30}")