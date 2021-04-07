from azure_file import azure_methodes
from application_logging.logger import App_Logger

class Template:

    """
        This class create all require directory through out the project .

        Written By: Tejas Dadhaniya
        Version: 1.0
        Revisions: None

        """

    def __init__(self):

     self.file_object = "templates"
     self.logger_object = App_Logger()
     self.azure = azure_methodes(logger_object=self.logger_object,file_object=self.file_object)
     self.dirs = [
         'fb-archive-bad-raw',
         'fb-archive-good-raw',
         'fb-bad-raw',
         'fb-good-raw',
         'fb-metadata',
         'fb-modelsforprediction',
         'fb-myrecycle-bin',
         'fb-predictedcsvfiles',
         'fb-prediction-archive-bad-raw',
         'fb-prediction-archive-good-raw',
         'fb-prediction-bad-raw',
         'fb-prediction-good-raw',
         'fb-predictionfilefromdb',
         'fb-predictionfiles',
         'fb-trainfiles',
         'fb-trainingfilefromdb'
     ]

    def directory(self):

     try:
         self.logger_object.log(file_object=self.file_object,
                                log_message="Enter in directory method of template class.")

         for dir_ in self.dirs:
            self.azure.create_container(dir_)

         self.logger_object.log(file_object=self.file_object,
                                log_message="Successfully created all directory..")

     except Exception as e:
         self.logger_object.log(file_object=self.file_object,log_message="Exception :: {} occurred template class.".format(e))


# m = Template()
# m.directory()