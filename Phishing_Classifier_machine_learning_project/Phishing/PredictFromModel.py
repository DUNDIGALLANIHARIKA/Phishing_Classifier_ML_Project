import pandas
from Phishing.File_Operations import File_Methods
from Phishing.Data_Preprocessing import Preprocessing
from Phishing.Data_Ingestion import Data_Loader_Prediction
from logging_app.logger import App_logger
from Phishing.Prediction_Raw_Data_Validation.PredictionDataValidation import Prediction_Data_Validation



class Prediction:

    def __init__(self,path):
        self.file_object = open("Phishing/Prediction_Logs/PredictionLog.txt", 'a+')
        self.logger = App_logger
        self.pred_data_val = Prediction_Data_Validation(path)

    def PredictionFromModel(self):

        try:
            self.pred_data_val.DeletePredictionFile() #deletes the existing prediction file from last run!
            self.logger.log(self.file_object,'Start of Prediction')
            data_getter=Data_Loader_Prediction.Data_Getter_Pred(self.file_object,self.logger)
            data=data_getter.Get_Data()

            preprocessor=Preprocessing.Preprocessor(self.file_object,self.logger)
            
            data = preprocessor.ReplaceInvalidValuesWithNull(data)

            is_null_present,cols_with_missing_values=preprocessor.IsNullPresent(data)
            if(is_null_present):
                data=preprocessor.Impute_Missing_Values(data,cols_with_missing_values)

            # get encoded values for categorical data
            #data = preprocessor.encodeCategoricalValuesPrediction(data)

            #data=data.to_numpy()
            file_loader=File_Methods.File_Operation(self.file_object,self.logger)
            kmeans=file_loader.Load_Model('KMeans')

            clusters=kmeans.predict(data)#drops the first column for cluster prediction
            data['clusters']=clusters
            clusters=data['clusters'].unique()
            result=[] # initialize blank list for storing predicitons
            # with open('EncoderPickle/enc.pickle', 'rb') as file: #let's load the encoder pickle file to decode the values
            #     encoder = pickle.load(file)

            for i in clusters:
                cluster_data= data[data['clusters']==i]
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                model_name = file_loader.Find_Correct_Model_File(i)
                model = file_loader.Load_Model(model_name)
                for val in (model.predict(cluster_data)):
                    result.append(val)
            result = pandas.DataFrame(result,columns=['Predictions'])
            path="Prediction_Output_File/Predictions.csv"
            result.to_csv("Prediction_Output_File/Predictions.csv",header=True) #appends result to prediction file
            self.logger.log(self.file_object,'End of Prediction')

        except Exception as ex:
            self.logger.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return path

           




