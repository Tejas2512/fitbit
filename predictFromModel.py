from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from application_logging import logger
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from azure_file import azure_methodes
import yaml,os
import pandas
from datetime import datetime

class prediction:

    def __init__(self,path):
        self.file_object = "Prediction_Log"
        self.log_writer = logger.App_Logger(database="Prediction_Logs")
        self.pred_data_val = Prediction_Data_validation(path)

        with open(os.path.join("configfile", "params.yaml"), "r") as f:
            self.configfile = yaml.safe_load(f)
        self.predictedfile = self.configfile["prediction"]["predictedfile"]

        with open(os.path.join("configfile","hyperparameter.yaml"),"r") as file:
            self.config = yaml.safe_load(file)
        self.drop_cols = self.config["columns"]["drop_cols"]
        self.label = self.config["columns"]["label_column_name"]
        self.random_state = self.config["base"]["random_state"]
        self.test_size = self.config["base"]["test_size"]

        self.azure_log = "Azurelog"
        self.azure = azure_methodes(file_object=self.azure_log, logger_object=self.log_writer)
        if path is not None:
            self.pred_data_val = Prediction_Data_validation(path)


    def predictionFromModel(self):

            try:
                #self.pred_data_val.deletePredictionFile() #deletes the existing prediction file from last run!
                self.log_writer.log(self.file_object,'Start of Prediction')
                data_getter=data_loader_prediction.Data_Getter_Pred(self.file_object,self.log_writer)
                data=data_getter.get_data()

                preprocessor=preprocessing.Preprocessor(self.file_object,self.log_writer)
                data = preprocessor.dropUnnecessaryColumns(data,self.drop_cols) #['Id','ActivityDate','TotalDistance','TrackerDistance']

                # replacing 'na' values with np.nan as discussed in the EDA part

                data = preprocessor.replaceInvalidValuesWithNull(data)

                is_null_present,cols_with_missing_values=preprocessor.is_null_present(data)
                if(is_null_present):
                    data=preprocessor.impute_missing_values(data)

                #scale the prediction data
                data_scaled = pandas.DataFrame(preprocessor.standardScalingData(data),columns=data.columns)

                #data=data.to_numpy()
                file_loader=file_methods.File_Operation(self.file_object,self.log_writer)
                kmeans=file_loader.load_model('KMeans')

                ##Code changed
                clusters=kmeans.predict(data_scaled)#drops the first column for cluster prediction
                data_scaled['clusters']=clusters
                clusters=data_scaled['clusters'].unique()
                result=[] # initialize blank list for storing predicitons

                for i in clusters:
                    cluster_data= data_scaled[data_scaled['clusters']==i]
                    cluster_data = cluster_data.drop(['clusters'],axis=1)
                    model_name = file_loader.find_correct_model_file(i)
                    model = file_loader.load_model(model_name)
                    for val in (model.predict(cluster_data.values)):
                        result.append(val)
                result = pandas.DataFrame(result,columns=['Predictions'])

                now = datetime.now()
                date = now.date()
                current_time = now.strftime("%H:%M:%S")

                filename = "Predictions_{}_{}.csv".format(date, current_time)
                self.azure.upload_blob(
                                       data=result.to_csv(index=False,header=True),
                                       container_name=self.predictedfile,
                                       blob_name=filename
                                       )

                self.log_writer.log(self.file_object,'End of Prediction')
                return "https://mlfitbit.blob.core.windows.net/fb-predictedcsvfiles/{}".format(filename)

            except Exception as ex:
                self.log_writer.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
                raise ex



