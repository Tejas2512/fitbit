from azure_file import azure_methodes
import os
import pandas
from application_logging.logger import App_Logger
import yaml

class dataTransform:

     """
               This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

               Written By: iNeuron Intelligence
               Version: 1.0
               Revisions: None

               """

     def __init__(self):
          with open(os.path.join("configfile", "params.yaml"), "r") as f:
               self.config = yaml.safe_load(f)
          self.goodDataPath = self.config["training"]["good_raw"]
          self.logger = App_Logger()
          self.azure_log = "Azurelog"
          self.azure = azure_methodes(file_object=self.azure_log, logger_object=self.logger)

     def addQuotesToStringValuesInColumn(self):
          """
                                           Method Name: replaceMissingWithNull
                                           Description: This method replaces the missing values in columns with "NULL" to
                                                        store in the table. We are using substring in the first column to
                                                        keep only "Integer" data for ease up the loading.
                                                        This column is anyways going to be removed during training.

                                            Written By: iNeuron Intelligence
                                           Version: 1.0
                                           Revisions: None

                                                   """

          log_file ="dataTransformLog"
          try:
               onlyfiles = self.azure.get_all_blob(container_name=self.goodDataPath)
               for file in onlyfiles:
                    storage_client = self.azure.connection()
                    csv = self.azure.read_blob(container_name=self.goodDataPath,blob_name=file)
                    csv.fillna('NULL',inplace=True)
                    self.azure.upload_blob(data=csv.to_csv(index=False,header=True),container_name=self.goodDataPath,blob_name=file)
                    self.logger.log(log_file, " %s: data transform successful!!" % file)
          except Exception as e:
               self.logger.log(log_file, "Data Transformation failed because:: %s" % e)

