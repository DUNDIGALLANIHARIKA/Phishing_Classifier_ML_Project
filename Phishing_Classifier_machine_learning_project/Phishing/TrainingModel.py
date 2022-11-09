from sklearn.model_selection import train_test_split
from Phishing.Data_Ingestion import Data_Loader
from Phishing.Data_Preprocessing import Preprocessing
from Phishing.Data_Preprocessing import Clustering
from Phishing.Best_Model_Finder import Tuner
from Phishing.File_Operations import File_Methods
from logging_app.logger import App_logger

class TrainModel:

    def __init__(self):
        self.logger = App_logger
        self.file_object = open("Phishing/Training_Logs/ModelTrainingLog.txt", 'a+')

    def TrainingModel(self):

        # Logging the start of Training
        self.logger.log(self.file_object, 'Start of Training')

        try:
            # Getting the data from the source
            data_getter = Data_Loader.Data_Getter(self.file_object,self.logger)
            data=data_getter.Get_Data()

            ##Data preprocessing
            preprocessor = Preprocessing.Preprocessor(self.file_object,self.logger)

            #data=preprocessor.remove_columns(data,['Wafer']) # remove the unnamed column as it doesn't contribute to prediction.

            #removing unwanted columns as discussed in the EDA part in ipynb file
            #data = preprocessor.dropUnnecessaryColumns(data,['veiltype'])

            #repalcing '?' values with np.nan as discussed in the EDA part

            data = preprocessor.ReplaceInvalidValuesWithNull(data)

            # check if missing values are present in the dataset
            is_null_present,cols_with_missing_values=preprocessor.IsNullPresent(data)

            # if missing values are there, replace them appropriately.
            if(is_null_present):
                data=preprocessor.Impute_Missing_Values(data,cols_with_missing_values) # missing value imputation

            # get encoded values for categorical data

            #data = preprocessor.encodeCategoricalValues(data)

            # create separate features and labels
            X, Y = preprocessor.Seperate_Label_Feature(data, label_column_name='Result')

            # drop the columns obtained above
            #X=preprocessor.remove_columns(X,cols_to_drop)

            """ Applying the clustering approach"""

            kmeans = Clustering.KMeansClustering(self.file_object,self.logger) # object initialization.
            number_of_clusters=kmeans.elbow_plot(X)  #  using the elbow plot to find the number of optimum clusters

            # Divide the data into clusters
            X=kmeans.Create_Clusters(X,number_of_clusters)

            #create a new column in the dataset consisting of the corresponding cluster assignments.
            X['Labels']=Y

            # getting the unique clusters from our dataset
            list_of_clusters=X['Cluster'].unique()

            """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

            for i in list_of_clusters:
                cluster_data=X[X['Cluster']==i] # filter the data for one cluster

                # Prepare the feature and Label columns
                cluster_features=cluster_data.drop(['Labels','Cluster'],axis=1)
                cluster_label= cluster_data['Labels']

                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3, random_state=36)

                model_finder = Tuner.Model_Finder(self.file_object,self.logger) # object initialization

                #getting the best model for each of the clusters
                best_model_name,best_model=model_finder.Get_Best_Model(x_train,y_train,x_test,y_test)

                #saving the best model to the directory.
                file_op = File_Methods.File_Operation(self.file_object,self.logger)
                save_model = file_op.Save_Model(best_model,best_model_name+str(i))

            # logging the successful Training
            self.logger.log(self.file_object, 'Successful End of Training')
            self.file_object.close()

        except Exception:
            # logging the unsuccessful Training
            self.logger.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception