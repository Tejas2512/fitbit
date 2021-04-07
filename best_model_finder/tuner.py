from sklearn.model_selection import GridSearchCV
from sklearn.metrics  import r2_score,mean_squared_error,mean_absolute_error
from sklearn.ensemble import RandomForestRegressor,VotingRegressor,BaggingRegressor,StackingRegressor
from sklearn.linear_model import LinearRegression,RidgeCV,Lasso
from xgboost import XGBRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.svm import SVR,LinearSVR
import numpy as np
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from datetime import datetime
import yaml,os

class Model_Finder:
    """
                This class shall  be used to find the model with best accuracy and AUC score.
                Written By: iNeuron Intelligence
                Version: 1.0
                Revisions: None

                """

    def __init__(self,file_object,logger_object):
        self.svr = SVR()
        self.knn = KNeighborsRegressor()
        self.file_object = file_object
        self.logger_object = logger_object
        self.linearReg = LinearRegression()
        self.RandomForestReg = RandomForestRegressor()
        self.xgb = XGBRegressor()
        self.linsvr = LinearSVR()
        self.dBOperation = dBOperation()

        with open(os.path.join("configfile","hyperparameter.yaml"),"r") as file:
            self.config = yaml.safe_load(file)
        self.verbose = self.config["gridsearch"]["verbose"]
        self.cv = self.config["gridsearch"]["cv"]

    def matrics(self,model,actual,predicted,cluster):
        self.logger_object.log(self.file_object, 'Entered the matrics method of the Model_Finder class')
        try:
            self.mean_absolute_error = mean_absolute_error(actual, predicted)
            self.mean_squared_error = mean_squared_error(actual, predicted)
            self.r2_score = r2_score(actual, predicted)
            self.now = datetime.now()
            self.date = self.now.date()
            self.current_time = self.now.strftime("%H:%M:%S")
            conn = self.dBOperation.createCollection(dataBase="Matrics", collectionName=model)
            dict_ = {"date":str(self.date),
                     "time":str(self.current_time),
                     "cluster":int(cluster),
                     "model_name":model,
                     "r2_score":float(self.r2_score),
                     "mean_squared_error":float(self.mean_squared_error),
                     "mean_absolute_error":float(self.mean_absolute_error)
                     }
            conn.insert_one(dict_)
            self.logger_object.log(self.file_object, "Matrices are evaluate successfully..!!")
            return self.r2_score

        except Exception as e:
            self.logger_object.log(self.file_object,"Exeption :: {} occurred in matrics methode of Model_Finder class.".format(e))
            raise e

    def kneighbors(self,train_x, train_y):
        self.logger_object.log(self.file_object,
                               'Entered the kneighbors method of the Model_Finder class')
        try:
            # initializing with different combination of parameters
            self.param_grid_knn = {
                                "n_neighbors": self.config["KNN"]["n_neighbors"],
                                "algorithm": self.config["KNN"]["algorithm"],
                                "p": self.config["KNN"]["p"],
                                "leaf_size": self.config["KNN"]["leaf_size"]
                                                     }

            # Creating an object of the Grid Search class
            self.grid = GridSearchCV(self.knn, self.param_grid_knn, verbose=self.verbose, cv=self.cv)
            # finding the best parameters
            self.grid.fit(train_x, train_y)

            # extracting the best parameters
            self.n_neighbors = self.grid.best_params_['n_neighbors']
            self.algorithm = self.grid.best_params_['algorithm']
            self.p = self.grid.best_params_['p']
            self.leaf_size = self.grid.best_params_['leaf_size']

            # creating a new model with the best parameters
            self.knnReg = KNeighborsRegressor(n_neighbors=self.n_neighbors, algorithm=self.algorithm,
                                                         p=self.p, leaf_size=self.leaf_size)
            # training the mew models
            self.knnReg.fit(train_x, train_y)
            self.logger_object.log(self.file_object,
                                   'kneighbors best params: ' + str(self.grid.best_params_) + '. Exited the kneighbors method of the Model_Finder class')
            return self.knnReg
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   "Exeption :: {} occurred in kneighbors methode of Model_Finder class.".format(e))
            self.logger_object.log(self.file_object,
                                   'kneighbors Parameter tuning  failed. Exited the kneighbors method of the Model_Finder class')
            raise e

    def suppor_vector(self,train_x, train_y):
        self.logger_object.log(self.file_object,
                               'Entered the suppor_vector method of the Model_Finder class')
        try:
            # initializing with different combination of parameters
            self.param_grid_svr = {
                "kernel": self.config["svm"]["kernel"],
                "tol": self.config["svm"]["tol"],
                "C": self.config["svm"]["C"],
                "epsilon": self.config["svm"]["epsilon"]
            }

            # Creating an object of the Grid Search class
            self.grid = GridSearchCV(self.svr, self.param_grid_svr, verbose=self.verbose, cv=self.cv)
            # finding the best parameters
            self.grid.fit(train_x, train_y)

            # extracting the best parameters
            self.kernel = self.grid.best_params_['kernel']
            self.tol = self.grid.best_params_['tol']
            self.C = self.grid.best_params_['C']
            self.epsilon = self.grid.best_params_['epsilon']

            # creating a new model with the best parameters
            self.svrReg = SVR(kernel=self.kernel, tol=self.tol,
                                              C=self.C, epsilon=self.epsilon)
            # training the mew models
            self.svrReg.fit(train_x, train_y)
            self.logger_object.log(self.file_object,
                                   'suppor_vector best params: ' + str(
                                       self.grid.best_params_) + '. Exited the suppor_vector method of the Model_Finder class')
            return self.svrReg
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   "Exeption :: {} occurred in suppor_vector methode of Model_Finder class.".format(e))
            self.logger_object.log(self.file_object,
                                   'suppor_vector Parameter tuning  failed. Exited the suppor_vector method of the Model_Finder class')
            raise e

    def voting_regressor(self,train_x, train_y):
        self.logger_object.log(self.file_object, 'Entered the voting_regressor method of the Model_Finder class')
        try:
            self.vote = VotingRegressor(estimators=([('lr', self.linearReg), ('rf', self.RandomForestReg),('svr', self.svr),('xgb', self.xgb)]))
            self.vote.fit(train_x,train_y)
            self.logger_object.log(self.file_object,
                                   'Successful. Exited the voting_regressor method of the Model_Finder class')
            return self.vote

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   "Exeption :: {} occurred in voting_regressor methode of Model_Finder class.".format(e))
            self.logger_object.log(self.file_object,
                                  'voting_regressor Parameter tuning  failed. Exited the voting_regressor method of the Model_Finder class')
            raise e

    def bagging_regressor(self, train_x, train_y):
        self.logger_object.log(self.file_object,'Entered the bagging_regressor method of the Model_Finder class')

        try:
            self.param_grid_bag = {
                "base_estimator": [LinearSVR(), SVR(), LinearRegression()],
                "n_estimators": self.config["bagging"]["n_estimators"],
            }
            self.bag = BaggingRegressor()
            # Creating an object of the Grid Search class
            self.grid = GridSearchCV(self.bag, self.param_grid_bag, verbose=self.verbose, cv=self.cv)
            # finding the best parameters
            self.grid.fit(train_x, train_y)

            # extracting the best parameters
            self.base_estimator = self.grid.best_params_['base_estimator']
            self.n_estimators = self.grid.best_params_['n_estimators']

            self.bagReg = BaggingRegressor(base_estimator=self.base_estimator
                                           ,n_estimators=self.n_estimators,
                                           bootstrap=self.config["bagging"]["bootstrap"],
                                           oob_score=self.config["bagging"]["oob_score"])
            self.bagReg.fit(train_x, train_y)
            self.logger_object.log(self.file_object,
                                   'bagging_regressor best params: ' + str(self.grid.best_params_) + '. Exited the bagging_regressor method of the Model_Finder class')
            return self.bagReg

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   "Exeption :: {} occurred in bagging_regressor methode of Model_Finder class.".format(e))
            self.logger_object.log(self.file_object,
                                   'bagging_regressor Parameter tuning  failed. Exited the bagging_regressor method of the Model_Finder class')
            raise e

    def stacking_regreesor(self,train_x, train_y):
        self.logger_object.log(self.file_object,'Entered the stacking_regreesor method of the Model_Finder class')
        try:
            self.estimator = [('Random Forest', self.RandomForestReg),('Lasso',Lasso() ),('xgb', self.xgb)]
            self.stackReg = StackingRegressor(estimators=self.estimator,final_estimator=RidgeCV())
            self.stackReg.fit(train_x,train_y)
            self.logger_object.log(self.file_object,
                                   'Successful. Exited the stacking_regreesor method of the Model_Finder class')
            return self.stackReg

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   "Exeption :: {} occurred in stacking_regreesor methode of Model_Finder class.".format(
                                       e))
            self.logger_object.log(self.file_object,
                                   'stacking_regreesor Parameter tuning  failed. Exited the stacking_regreesor method of the Model_Finder class')
            raise e

    def get_best_params_for_Random_Forest_Regressor(self, train_x, train_y):
        """
                                                Method Name: get_best_params_for_Random_Forest_Regressor
                                                Description: get the parameters for Random_Forest_Regressor Algorithm which give the best accuracy.
                                                             Use Hyper Parameter Tuning.
                                                Output: The model with the best parameters
                                                On Failure: Raise Exception

                                                Written By: iNeuron Intelligence
                                                Version: 1.0
                                                Revisions: None

                                        """
        self.logger_object.log(self.file_object,
                               'Entered the RandomForestReg method of the Model_Finder class')
        try:
            # initializing with different combination of parameters
            self.param_grid_Random_forest_Tree = {
                                "n_estimators": self.config["random_forest"]["n_estimators"],
                                "max_features": self.config["random_forest"]["max_features"],
                                "min_samples_split": self.config["random_forest"]["min_samples_split"],
                                "bootstrap": self.config["random_forest"]["bootstrap"]
                                                     }

            # Creating an object of the Grid Search class
            self.grid = GridSearchCV(self.RandomForestReg, self.param_grid_Random_forest_Tree, verbose=self.verbose, cv=self.cv)
            # finding the best parameters
            self.grid.fit(train_x, train_y)

            # extracting the best parameters
            self.n_estimators = self.grid.best_params_['n_estimators']
            self.max_features = self.grid.best_params_['max_features']
            self.min_samples_split = self.grid.best_params_['min_samples_split']
            self.bootstrap = self.grid.best_params_['bootstrap']

            # creating a new model with the best parameters
            self.decisionTreeReg = RandomForestRegressor(n_estimators=self.n_estimators, max_features=self.max_features,
                                                         min_samples_split=self.min_samples_split, bootstrap=self.bootstrap)
            # training the mew models
            self.decisionTreeReg.fit(train_x, train_y)
            self.logger_object.log(self.file_object,
                                   'RandomForestReg best params: ' + str(
                                       self.grid.best_params_) + '. Exited the RandomForestReg method of the Model_Finder class')
            return self.decisionTreeReg
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in RandomForestReg method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'RandomForestReg Parameter tuning  failed. Exited the get_best_params_for_Random_Forest_Regressor method of the Model_Finder class')
            raise Exception()

    def get_best_params_for_linearReg(self,train_x,train_y):

        """
                                        Method Name: get_best_params_for_linearReg
                                        Description: get the parameters for LinearReg Algorithm which give the best accuracy.
                                                     Use Hyper Parameter Tuning.
                                        Output: The model with the best parameters
                                        On Failure: Raise Exception

                                        Written By: iNeuron Intelligence
                                        Version: 1.0
                                        Revisions: None

                                """
        self.logger_object.log(self.file_object,
                               'Entered the get_best_params_for_linearReg method of the Model_Finder class')
        try:
            # initializing with different combination of parameters
            self.param_grid_linearReg = {
                'fit_intercept': self.config["linear_regression"]["fit_intercept"]
                , 'normalize': self.config["linear_regression"]["normalize"],
                'copy_X': self.config["linear_regression"]["copy_X"]

            }
            # Creating an object of the Grid Search class
            self.grid= GridSearchCV(self.linearReg,self.param_grid_linearReg, verbose=3,cv=5)
            # finding the best parameters
            self.grid.fit(train_x, train_y)

            # extracting the best parameters
            self.fit_intercept = self.grid.best_params_['fit_intercept']
            self.normalize = self.grid.best_params_['normalize']
            self.copy_X = self.grid.best_params_['copy_X']

            # creating a new model with the best parameters
            self.linReg = LinearRegression(fit_intercept=self.fit_intercept,normalize=self.normalize,copy_X=self.copy_X)
            # training the mew model
            self.linReg.fit(train_x, train_y)
            self.logger_object.log(self.file_object,
                                   'LinearRegression best params: ' + str(
                                       self.grid.best_params_) + '. Exited the get_best_params_for_linearReg method of the Model_Finder class')
            return self.linReg
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_params_for_linearReg method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'LinearReg Parameter tuning  failed. Exited the get_best_params_for_linearReg method of the Model_Finder class')
            raise Exception()

    def get_best_model(self,train_x,train_y,test_x,test_y,cluster):
        """
                                                Method Name: get_best_model
                                                Description: Find out the Model which has the best AUC score.
                                                Output: The best model name and the model object
                                                On Failure: Raise Exception

                                                Written By: iNeuron Intelligence
                                                Version: 1.0
                                                Revisions: None

                                        """
        self.logger_object.log(self.file_object,
                               'Entered the get_best_model method of the Model_Finder class')
        # create best model for Linear Regression
        try:

            self.KNN = self.kneighbors(train_x,train_y)
            self.prediction_knn = self.KNN.predict(test_x)
            self.KNN_r2 = self.matrics(model="KNN",actual=test_y,predicted=self.prediction_knn,cluster=cluster)

            self.SVR = self.suppor_vector(train_x,train_y)
            self.prediction_svr = self.SVR.predict(test_x)
            self.SVR_r2 = self.matrics(model="SVR",actual=test_y,predicted=self.prediction_svr,cluster=cluster)

            self.VR = self.voting_regressor(train_x,train_y)
            self.prediction_vr = self.VR.predict(test_x)
            self.VR_r2 = self.matrics(model="Voting_regressor",actual=test_y,predicted=self.prediction_vr,cluster=cluster)

            self.BagReg = self.bagging_regressor(train_x, train_y)
            self.prediction_br = self.BagReg.predict(test_x)
            self.BagReg_r2 = self.matrics(model="Bagging_regressor",actual=test_y, predicted=self.prediction_br,cluster=cluster)

            self.StackReg = self.stacking_regreesor(train_x,train_y)
            self.prediction_sr = self.StackReg.predict(test_x)
            self.StackReg_r2 = self.matrics(model="Stacking_regreesor",actual=test_y,predicted=self.prediction_sr,cluster=cluster)

            self.LinearReg= self.get_best_params_for_linearReg(train_x, train_y)
            self.prediction_LinearReg = self.LinearReg.predict(test_x) # Predictions using the LinearReg Model
            self.LinearReg_r2 = self.matrics(model="Linear_Regression",actual=test_y,predicted=self.prediction_LinearReg,cluster=cluster)#r2_score(test_y,self.prediction_LinearReg)

            self.randomForestReg = self.get_best_params_for_Random_Forest_Regressor(train_x, train_y)
            self.prediction_randomForestReg = self.randomForestReg.predict(test_x)  # Predictions using the randomForestReg Model
            self.randomForestReg_r2 = self.matrics(model="RandomForest_Regressor",actual=test_y,predicted=self.prediction_randomForestReg,cluster=cluster) #r2_score(test_y,self.prediction_randomForestReg)

            name = ["KNN", "SVR", "Voting_Regressor", "Bagging_Regressor", "Stacking_Regreesor","Linear_Regression","RandomForest_Regressor"]
            obj = [self.KNN, self.SVR, self.VR, self.BagReg, self.StackReg,self.LinearReg,self.randomForestReg]
            score = [self.KNN_r2, self.SVR_r2, self.VR_r2,self.BagReg_r2, self.StackReg_r2,self.LinearReg_r2,self.randomForestReg_r2]

            model = name[np.array(score).argmax()]
            model_obj = obj[np.array(score).argmax()]

            return model,model_obj

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_model method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise Exception()

