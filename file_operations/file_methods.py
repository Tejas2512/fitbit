import pickle,yaml
import os
from azure_file import azure_methodes

class File_Operation:
    """
                This class shall be used to save the model after training
                and load the saved model for prediction.

                Written By: iNeuron Intelligence
                Version: 1.0
                Revisions: None

                """
    def __init__(self,file_object,logger_object):

        self.file_object = file_object
        self.logger_object = logger_object
        with open(os.path.join("configfile","params.yaml"),"r") as f:
            self.config = yaml.safe_load(f)
        self.model_directory = self.config["modeltraining"]["bucket"]
        self.log = "Azurelog"
        self.azure = azure_methodes(file_object=self.log,logger_object=self.logger_object)

    def save_model(self,model,filename):
        """
                    Method Name: save_model
                    Description: Save the model file to directory
                    Outcome: File gets saved
                    On Failure: Raise Exception

                    Written By: iNeuron Intelligence
                    Version: 1.0
                    Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the save_model method of the File_Operation class')
        try:
            model = pickle.dumps(model)
            self.azure.upload_blob(data=model,container_name=self.model_directory,blob_name=filename + ".sav")

            # storage_client = self.gcp.connection()
            # bucket = storage_client.get_bucket(self.model_directory)
            # bucket.blob(filename + ".sav").upload_from_string(pickle.dumps(model), 'text/sav')
            self.logger_object.log(self.file_object,
                                   'Model File ' + filename + ' saved. Exited the save_model method of the File_Operation class')

            return 'success'
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in save_model method of the File_Operation class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Model File ' + filename + ' could not be saved. Exited the save_model method of the File_Operation class')
            raise Exception()

    def load_model(self,filename):
        """
                            Method Name: load_model
                            Description: load the model file to memory
                            Output: The Model file loaded in memory
                            On Failure: Raise Exception

                            Written By: iNeuron Intelligence
                            Version: 1.0
                            Revisions: None
                """
        self.logger_object.log(self.file_object, 'Entered the load_model method of the File_Operation class')
        try:

            model = self.azure.read_pickel(container_name=self.model_directory,blob_name=filename+".sav")

            # storage_client = self.gcp.connection()
            # bucket = storage_client.get_bucket(self.model_directory)
            # blob = bucket.blob(filename + ".sav")
            # model = pickle.loads(blob.download_as_bytes())
            return model

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in load_model method of the File_Operation class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,
                                   'Model File ' + filename + ' could not be saved. Exited the load_model method of the File_Operation class')
            raise Exception()

    def find_correct_model_file(self,cluster_number):
        """
                            Method Name: find_correct_model_file
                            Description: Select the correct model based on cluster number
                            Output: The Model file
                            On Failure: Raise Exception

                            Written By: iNeuron Intelligence
                            Version: 1.0
                            Revisions: None
                """
        self.logger_object.log(self.file_object, 'Entered the find_correct_model_file method of the File_Operation class')
        try:
            self.cluster_number= cluster_number
            self.folder_name=self.model_directory
            self.list_of_files = self.azure.get_all_blob(container_name=self.folder_name)
            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str( self.cluster_number))!=-1):
                        self.model_name=self.file
                except:
                    continue
            self.model_name=self.model_name.split('.')[0]
            self.logger_object.log(self.file_object,
                                   'Exited the find_correct_model_file method of the Model_Finder class.')
            return self.model_name
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in find_correct_model_file method of the File_Operation class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Exited the find_correct_model_file method of the File_Operation class with Failure')
            raise Exception()