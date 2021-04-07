import os,yaml
import re
import pandas as pd
from application_logging.logger import App_Logger
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import dBOperation
import smtplib
from azure_file import azure_methodes

class Prediction_Data_validation:
    """
               This class shall be used for handling all the validation done on the Raw Prediction Data!!.

               Written By: iNeuron Intelligence
               Version: 1.0
               Revisions: None

               """

    def __init__(self,path):

        self.db_operation = dBOperation()
        self.Batch_Directory = path
        self.logger = App_Logger(database="Prediction_Logs")
        self.gcp_log = "Azurelog"
        self.azure = azure_methodes(file_object=self.gcp_log, logger_object=self.logger)

        print("yaml open")
        with open(os.path.join("configfile", "params.yaml"), "r") as f:
            self.config = yaml.safe_load(f)
            print("yaml done")
        self.predictionfile = self.config["prediction"]["bucket"]
        self.good_raw = self.config["prediction"]["good_raw"]
        self.bad_raw = self.config["prediction"]["bad_raw"]
        self.archive_good_raw = self.config["prediction"]["archive_good_raw"]
        self.archive_bad_raw = self.config["prediction"]["archive_bad_raw"]

    def valuesFromSchema(self):
            """
                                    Method Name: valuesFromSchema
                                    Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                                    Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                                    On Failure: Raise ValueError,KeyError,Exception

                                     Written By: iNeuron Intelligence
                                    Version: 1.0
                                    Revisions: None

                                            """
            file = "valuesfromSchemaValidationLog"
            try:
                client = self.db_operation.dataBaseConnection()
                database = client["schema"]
                schema = database["Prediction_schema"]
                df = pd.DataFrame(schema.find())

                LengthOfDateStampInFile = int(df['LengthOfDateStampInFile'])
                LengthOfTimeStampInFile = int(df['LengthOfTimeStampInFile'])
                NumberofColumns = int(df['NumberofColumns'])
                column_names = df['ColName']

                message = "LengthOfDateStampInFile:: %s" % LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile + "\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
                self.logger.log(file, message)

            except ValueError:
                file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
                self.logger.log(file,"ValueError:Value not found inside schema_training.json")
                file.close()
                raise ValueError

            except KeyError:
                file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
                self.logger.log(file, "KeyError:Key value error incorrect key passed")
                file.close()
                raise KeyError

            except Exception as e:
                file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
                self.logger.log(file, str(e))
                file.close()
                raise e

            return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns

    def manualRegexCreation(self):

        """
                                      Method Name: manualRegexCreation
                                      Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                                  This Regex is used to validate the filename of the prediction data.
                                      Output: Regex pattern
                                      On Failure: None

                                       Written By: iNeuron Intelligence
                                      Version: 1.0
                                      Revisions: None

                                              """
        regex = "['FitBit']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def deleteExistingGoodDataTrainingFolder(self):
        """
                                             Method Name: deleteExistingGoodDataTrainingFolder
                                             Description: This method deletes the directory made  to store the Good Data
                                                           after loading the data in the table. Once the good files are
                                                           loaded in the DB,deleting the directory ensures space optimization.
                                             Output: None
                                             On Failure: raise exception

                                              Written By: Tejas Dadhaniya
                                             Version: 1.0
                                             Revisions: None

                                                     """

        try:
            # self.gcp.create_bucket("cs_archive_good_raw")
            self.azure.move_blob(source_container=self.good_raw,destination_container=self.archive_good_raw)
            file = "GeneralLog"
            self.logger.log(file, "All files in GoodRaw bucket deleted successfully!!!")
        except Exception as e:
            file = "GeneralLog"
            self.logger.log(file, "Error while Deleting files from good raw bucket : %s" % e)
            raise e

    def deleteExistingBadDataTrainingFolder(self):

        """
                                                           Method Name: deleteExistingBadDataTrainingFolder
                                                           Description: This method deletes the directory made to store the bad Data.
                                                           Output: None
                                                           On Failure: raise exception

                                                            Written By: Tejas Dadhaniya
                                                           Version: 1.0
                                                           Revisions: None

                                                                   """

        try:
            # self.gcp.create_bucket("cs_archive_bad_raw")
            self.azure.move_blob(source_container=self.bad_raw, destination_container=self.archive_bad_raw)
            file = "GeneralLog"
            self.logger.log(file, "All files in BadRaw bucket deleted before starting validation!!!")
        except Exception as e:
            file = "GeneralLog"
            self.logger.log(file, "Error while Deleting files from bad raw : %s" % e)
            raise e

    def notification(self, lst):
        pass

    def moveBadFilesToArchiveBad(self):



        """
                                                   Method Name: moveBadFilesToArchiveBad
                                                   Description: This method deletes the directory made  to store the Bad Data
                                                                 after moving the data in an archive folder. We archive the bad
                                                                 files to send them back to the client for invalid data issue.
                                                   Output: None
                                                   On Failure: raise exeption

                                                    Written By: Tejas Dadhaniya
                                                   Version: 1.0
                                                   Revisions: None

                                                           """

        try:
            lst = self.azure.get_all_blob(container_name=self.bad_raw)
            f = "ArchivedBadFile"

            self.notification(lst)
            self.deleteExistingBadDataTrainingFolder()
            self.logger.log(f, "moving bad files to archive and files are :: {}".format(lst))

        except Exception as e:
            file = "GeneralLog"
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            raise e

    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):

        """
                    Method Name: validationFileNameRaw
                    Description: This function validates the name of the training csv files as per given name in the schema!
                                 Regex pattern is used to do the validation.If name format do not match the file is moved
                                 to Bad Raw Data folder else in Good raw data.
                    Output: None
                    On Failure: Exception

                     Written By: iNeuron Intelligence
                    Version: 1.0
                    Revisions: None

                """


        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingGoodDataTrainingFolder()
        self.deleteExistingBadDataTrainingFolder()

        f = "validationFileNameRaw"
        onlyfiles = self.azure.get_all_blob(container_name=self.predictionfile)

        try:
            # # create new directories
            # self.createDirectoryForGoodBadRawData()
            # f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            self.azure.copy_blob(source_container=self.predictionfile,source_blob=filename,destination_container=self.good_raw,blob_name=filename)
                            #shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            self.azure.copy_blob(source_container=self.predictionfile,source_blob=filename,destination_container=self.bad_raw,blob_name=filename)
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        self.azure.copy_blob(source_container=self.predictionfile,source_blob=filename, destination_container=self.bad_raw,
                                             blob_name=filename)
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    self.azure.copy_blob(source_container=self.predictionfile,source_blob=filename, destination_container=self.bad_raw,
                                         blob_name=filename)
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

        except Exception as e:
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            raise e

    def validateColumnLength(self,NumberofColumns):
        """
                          Method Name: validateColumnLength
                          Description: This function validates the number of columns in the csv files.
                                       It is should be same as given in the schema file.
                                       If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                       If the column number matches, file is kept in Good Raw Data for processing.
                                      The csv file is missing the first column name, this function changes the missing name to "creditCardFraud".
                          Output: None
                          On Failure: Exception

                           Written By: iNeuron Intelligence
                          Version: 1.0
                          Revisions: None

                      """
        f = "validateColumnLength"
        try:
            self.logger.log(f,"Column Length Validation Started!!")
            onlyfiles = self.azure.get_all_blob(container_name=self.good_raw)

            for file in onlyfiles:
                csv = self.azure.read_blob(container_name=self.good_raw,blob_name=file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    self.azure.move_blob(source_container=self.good_raw,destination_container=self.bad_raw,blob_name=file,copy_type="single")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")

        except Exception as e:
            self.logger.log(f, "Error Occured:: %s" % e)
            raise e

    def deletePredictionFile(self):

        if os.path.exists('Prediction_Output_File/Predictions.csv'):
            os.remove('Prediction_Output_File/Predictions.csv')

    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception

                                   Written By: iNeuron Intelligence
                                  Version: 1.0
                                  Revisions: None

                              """
        f = "missingValuesInColumn"
        try:
            self.logger.log(f,"Missing Values Validation Started!!")
            onlyfiles = self.azure.get_all_blob(container_name=self.good_raw)

            for file in onlyfiles:

                csv = self.azure.read_blob(container_name=self.good_raw,
                                           blob_name=file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        self.azure.move_blob(source_container=self.good_raw, destination_container=self.bad_raw,
                                             blob_name=file, copy_type="single")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break

        except Exception as e:
            self.logger.log(f, "Error Occured:: %s" % e)
            raise e