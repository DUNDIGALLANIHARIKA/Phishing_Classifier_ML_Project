from datetime import datetime
from Phishing.Training_Raw_Data_Validation.RawValidation import Raw_Data_Validation
from Phishing.DataTypeValidation_Insertion_Training.DataTypeValidation import DBOperation
from Phishing.DataTransform_Training.DataTransformation import DataTransform
from logging_app.logger import App_logger

class Train_Validation:
    def __init__(self,path):
        self.raw_data = Raw_Data_Validation(path)
        self.dataTransform = DataTransform()
        self.dbOperation = DBOperation()
        self.file_object = open("Phishing/Training_Logs/Training_Main_Log.txt",'a+')
        self.logger = App_logger

    def Train_Validation(self):

        try:
            self.logger.log(self.file_object,'Start Validation on files for prediction!!')

            ##extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NoOfColumns = self.raw_data.ValuesFromSchema()

            # getting the regex defined to validate filename
            regex = self.raw_data.ManualRegexCreation()

            # validating filename of prediction files
            self.raw_data.ValidationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)

            # validating column length in the file
            self.raw_data.validateColumnLength(NoOfColumns)

            # validating if any column has all values missing
            self.raw_data.ValidateMissingValuesInWholeColumn()
            self.logger.log(self.file_object, "Raw Data Validation Complete!!")
            self.logger.log(self.file_object, "Starting Data Transforamtion!!")

            # below function adds quotes to the '?' values in some columns.
            self.dataTransform.AddQuotesToStringValuesInColumn()
            self.logger.log(self.file_object, "DataTransformation Completed!!!")
            self.logger.log(self.file_object,"Creating Training_Database and tables on the basis of given schema!!!")

            ##create database with given name, if present open the connection! Create table with columns given in schema
            self.dbOperation.CreateTableDB('Training', column_names)
            self.logger.log(self.file_object, "Table creation Completed!!")
            self.logger.log(self.file_object, "Insertion of Data in the Table started!!!!")

            # insert csv files in the table
            self.dbOperation.InsertGoodDataIntoTable('Training')
            self.logger.log(self.file_object, "Insertion in Table completed!!!")
            self.logger.log(self.file_object, "Deleting Good Data Folder!!!")

            # Delete the good data folder after loading files in table
            self.raw_data.DeleteExistingGoodDataTrainingFolder()
            self.logger.log(self.file_object, "Good_Data folder deleted!!!")
            self.logger.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")

            # Move the bad files to archive folder
            self.raw_data.MoveBadFilesToArchieveBad()
            self.logger.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.logger.log(self.file_object, "Validation Operation completed!!")
            self.logger.log(self.file_object, "Extracting csv file from table")

            # export data in table to csvfile
            self.dbOperation.SelectingDataFromTableIntoCSV('Training')
            self.file_object.close()

        except Exception as e:
            raise e 


