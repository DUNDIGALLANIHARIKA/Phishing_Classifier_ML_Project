from Phishing.Prediction_Raw_Data_Validation.PredictionDataValidation import Prediction_Data_Validation
from Phishing.DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import DBOperation
from Phishing.DataTransformation_Prediction.DataTransformationPrediction import DataTransformPredict
from logging_app.logger import App_logger

class Pred_Validation:
    def __init__(self,path):
        self.raw_data = Prediction_Data_Validation(path)
        self.dataTransform = DataTransformPredict()
        self.dBOperation = DBOperation()
        self.file_object = open("Phishing/Prediction_Logs/PredictionLog.txt", 'a+')
        self.logger = App_logger

    def Prediction_Validation(self):

        try:

            self.logger.log(self.file_object,'Start of Validation on files for prediction!!')

            #extracting values from prediction schema
            LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,NoOfColumns = self.raw_data.ValuesFromSchema()

            #getting the regex defined to validate filename
            regex = self.raw_data.ManualRegexCreation()

            #validating filename of prediction files
            self.raw_data.ValidationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
        
            #validating column length in the file
            self.raw_data.ValidateColumnLength(NoOfColumns)

            #validating if any column has all values missing
            self.raw_data.ValidateMissingValuesInWholeColumn()
            self.logger.log(self.file_object,"Raw Data Validation Complete!!")

            self.logger.log(self.file_object,("Starting Data Transforamtion!!"))

            #replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.AddQuotesToStringValuesInColumn()
            self.logger.log(self.file_object,"DataTransformation Completed!!!")
            self.logger.log(self.file_object,"Creating Prediction_Database and tables on the basis of given schema!!!")

            #create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.CreateTableDB('Prediction',column_names)
            self.logger.log(self.file_object,"Table creation Completed!!")
            self.logger.log(self.file_object,"Insertion of Data into Table started!!!!")

            #insert csv files in the table
            self.dBOperation.InsertIntoTableGoodData('Prediction')
            self.logger.log(self.file_object,"Insertion in Table completed!!!")
            self.logger.log(self.file_object,"Deleting Good Data Folder!!!")

            #Delete the good data folder after loading files in table
            self.raw_data.DeleteExistingGoodDataTrainingFolder()
            self.logger.log(self.file_object,"Good_Data folder deleted!!!")
            self.logger.log(self.file_object,"Moving bad files to Archive and deleting Bad_Data folder!!!")

            #Move the bad files to archive folder
            self.raw_data.MoveBadFilesToArchiveBad()
            self.logger.log(self.file_object,"Bad files moved to archive!! Bad folder Deleted!!")
            self.logger.log(self.file_object,"Validation Operation completed!!")
            self.logger.log(self.file_object,"Extracting csv file from table")

            #export data in table to csvfile
            self.dBOperation.SelectingDataFromtableIntoCSV('Prediction')

        except Exception as e:
            raise e
