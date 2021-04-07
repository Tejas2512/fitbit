import pandas as pd
from azure_file import azure_methodes
import os,yaml

class Data_Getter_Pred:
    """
    This class shall  be used for obtaining the data from the source for prediction.

    Written By: iNeuron Intelligence
    Version: 1.0
    Revisions: None

    """
    def __init__(self, file_object, logger_object):

        self.file_object = file_object
        self.logger_object = logger_object
        self.file = "Azurelog"
        self.azure = azure_methodes(file_object=self.file,logger_object= self.logger_object)

        with open(os.path.join("configfile", "params.yaml"), "r") as f:
            self.config = yaml.safe_load(f)
        self.predictionfilefromdb = self.config['prediction']['predictionfilefromdb']
        self.predictionfile = self.config["training"]["mastercsv"]

    def get_data(self):
        """
        Method Name: get_data
        Description: This method reads the data from source.
        Output: A pandas DataFrame.
        On Failure: Raise Exception

         Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None

        """
        self.logger_object.log(self.file_object,'Entered the get_data method of the Data_Getter class')
        try:
            self.data= self.azure.read_blob(container_name=self.predictionfilefromdb,blob_name=self.predictionfile)
            #pd.read_csv(self.prediction_file) # reading the data file
            self.logger_object.log(self.file_object,'Data Load Successful.Exited the get_data method of the Data_Getter class')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_data method of the Data_Getter class. Exception message: '+str(e))
            self.logger_object.log(self.file_object,
                                   'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise Exception()


