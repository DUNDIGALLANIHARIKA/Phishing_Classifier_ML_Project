import sqlite3
from datetime import datetime 
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from logging_app.logger import App_logger

class Prediction_Data_Validation:
    """
        This class is used for validating the raw data.
    """

    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_path = 'config/schema_prediction.json'
        self.logger = App_logger

    def ValuesFromSchema(self):
        """
            This function extracts all the relevant information from the pre-defined "Schema" file.
        """

        try:
            with open(self.schema_path,'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberOfColumns = dic['NumberOfColumns']

            file = open("Phishing/Training_Logs/valuesfromSchemaValidationLog.txt",'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberOfColumns + "\n"
            self.logger.log(file,message)

            file.close()

        except ValueError:
            file = open("Phishing/Prediction_Logs/ValuesFromSchemaValidationLog.txt",'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Phishing/Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Phishing/Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberOfColumns

    def ManualRegexCreation(self):
        """
            This method is based on regex which helps in validating the filename which is present in  "schema" file.
        """

        regex = "['phishing']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):
        """
            This function creates directories for good and bad data after validating training data.
        """
        
        try:
            path = os.path.join("Phishing/Prediction_Raw_Files_Validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Phishng/Prediction_Raw_Files_Validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Phishing/Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while creating Directory %s:" % ex)
            file.close()
            raise OSError

    def DeleteExistingGoodDataTrainingFolder(self):
        """
            This method helps in deleting the existing folder of good data which is stored in database, so that directory ensures 
            space optimization.                                   
        """

        try:
            path = 'Phishing/Prediction_Raw_Files_Validated/'
            # if os.path.isdir("ids/" + userName):
            # if os.path.isdir(path + 'Bad_Raw/'):
            #     shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("Phishing/Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()

        except OSError as s:
            file = open("Phishing/Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError

    def DeleteExistingBadDataTrainingFolder(self):
        """
            This method helps in deleting the existing folder of bad data which is stored in database, so that directory ensures 
            space optimization.                                   
        """

        try:
            path = 'Phishing/Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Phishing/Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()

        except OSError as s:
            file = open("Phishing/Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError

    def MoveBadFilesToArchiveBad(self):
        """
            This method deletes the directory where the bad data is stored after moving the data to archieve folder.
            The archieve bad data will be sent to client for invalid data.                                    
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")

        try:
            path= "Phishing/PredictionArchivedBadData"

            if not os.path.isdir(path):
                os.makedirs(path)
            source = 'Phishing/Prediction_Raw_Files_Validated/Bad_Raw/'
            dest = 'Phishing/PredictionArchivedBadData/BadData_' + str(date)+"_"+str(time)
            if not os.path.isdir(dest):
                os.makedirs(dest)

            files = os.listdir(source)
            for f in files:
                if f not in os.listdir(dest):
                    shutil.move(source + f, dest)

            file = open("Phishing/Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Bad files moved to archive")
            path = 'Phishing/Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
            self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
            file.close()

        except OSError as e:
            file = open("Phishing/Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise OSError


    def ValidationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
            This function validates the name of the training csv files as per given name in the schema.Regex pattern is used to do the
            validation.If name format do not match the file is moved to Bad Raw Data folder else in Good raw data.
        """

        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.DeleteExistingBadDataTrainingFolder()
        self.DeleteExistingGoodDataTrainingFolder()
        self.createDirectoryForGoodBadRawData()

        onlyfiles = [f for f in listdir(self.Batch_Directory)]

        try:
            f = open("Phishing/Prediction_Logs/NameValidationLog.txt", 'a+')

            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Phishing/Prediction_Batch_Files/" + filename, "Phishing/Prediction_Raw_Files_Validated/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("Phishing/Prediction_Batch_Files/" + filename, "Phishing/Prediction_Raw_Files_Validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("Phishing/Prediction_Batch_Files/" + filename, "Phishing/Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("Phishing/Prediction_Batch_Files/" + filename, "Phishing/Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("Phishing/Prediction_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e

    def ValidateColumnLength(self,NumberOfColumns):
        """
            This function helps in validating number of columns in csv as same as schema file. If the data is same and nmber of columns 
            are same then data is sent to good raw data folder.Otherwise,bad raw data folder. 
        """

        try:
            f = open("Phishing/Prediction_Logs/ColumnValidationLog.txt", 'a+')
            self.logger.log(f,"Column Length Validation Started!!")
            for file in listdir('Phishing/Prediction_Raw_Files_Validated/Good_Raw/'):
                csv = pd.read_csv("Phishing/Prediction_Raw_Files_Validated/Good_Raw/" + file)
                if csv.shape[1] == NumberOfColumns:
                    csv.to_csv("Phishing/Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)
                else:
                    shutil.move("Phishing/Prediction_Raw_Files_Validated/Good_Raw/" + file, "Phishing/Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)

            self.logger.log(f, "Column Length Validation Completed!!")

        except OSError:
            f = open("Phishing/Prediction_Logs/ColumnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError

        except Exception as e:
            f = open("Phishing/Prediction_Logs/ColumnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e

        f.close()

    def DeletePredictionFile(self):

        if os.path.exists('Phishing/Prediction_Output_File/Predictions.csv'):
            os.remove('Phishing/Prediction_Output_File/Predictions.csv')

    def ValidateMissingValuesInWholeColumn(self):
        """
            This function checks whether any column is missing or not. If any column is missing then, the data is sent to bad raw data .
            As, further , the data cannot be processed for preprocessing.            
        """

        try:
            f = open("Phishing/Prediction_Logs/MissingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started!!")

            for file in listdir('Phishing/Prediction_Raw_Files_Validated/Good_Raw/'):
                csv = pd.read_csv("Phishing/Prediction_Raw_Files_Validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move("Phishing/Prediction_Raw_Files_Validated/Good_Raw/" + file,"Phishing/Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.to_csv("Phishing/Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)

        except OSError:
            f = open("Phishing/Prediction_Logs/MissingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError

        except Exception as e:
            f = open("Phishing/Prediction_Logs/MissingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
            
        f.close()

