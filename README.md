### Automate entire machine learning pipeline and create end to end solution for given project.

**Project** : _To build a regression model to predict the calories burnt based on the given indicators in the
training data._

**Approach:** 

Entire project divided into following pipelines:

(1) Data collection (2) Data validation (3) Data insertion in Database (4) Data pre-processing (5) Model training and prediction 

`Data collection`

Data had been coming from some remote sources like Azure storage, GCP storage, or AWS S3 bucket.  

`Data validation` 

In this stage file name, columns name, columns length and missing values in columns (if entire column has null value then drop it) is validate according to training and prediction schema. Files passed all validation steps moved to good data container and file failed in data validation moved to bad data container and send email to client with rejected files.

`Data insertion in Database` 

After data validation we store all good files (files successfully passed all validation steps) into MongoDB atlas collection and create final master csv file for prediction. After that we export master csv file to azure container.

`Data pre-processing` 

In data pre-processing we performed feature scaling, drop column with 0 standard deviation, drop duplicated rows, separate dependant and independent features, and train-test split.

`Model training `

In model training we grouped data has same pattern using KMeans clustering and label each data with cluster number. Then we trained each cluster on different algorithms (Stacking, bagging, boosting, SVM, KNN etc.), algorithms with highest r2_score get saved with <model_name><cluster_name>.sav in azure container.  This step repeat number of cluster times.

`Prediction`

All the steps (exclude model training) performed during prediction also. According to cluster number pretrained model get selected for prediction. At end of prediction prediction.csv file saved in azure container and path appear in application.   

`Tools & Libraries`

Language: `python3.6`

Tools: `Docker` (Build docker image)

Cloud:` Azure` (Store training files, model files, prediction files and metadata)

Database: `MongoDB atlas` (Store Logs, Schemas, Evolution matrices and Metafiles)

Libraries: `sklearn`, `pandas`, `NumPy`, `flask`,` azure-storage`, `pymongo` etc.


###### ** Detail explanation provided in _**Problem Statement.docx**_ file.

### Command we use to build docker image:

`docker image build -t <REPOSITORY>` 

`docker images`

`docker ps `

`docker run -p 5000:5000 -d <REPOSITORY>`

`docker login dockerfitbit.azurecr.io`

`docker push dockerfitbit.azurecr.io/mlfitbit:latest`

`docker stop <containerID>`

`docker system prune`

