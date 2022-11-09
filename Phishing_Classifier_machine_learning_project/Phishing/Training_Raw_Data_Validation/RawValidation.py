from email import message
import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
from tokenize import Number
import pandas as pd
from logging_app.logger import App_logger



class Raw_Data_Validation:

    def __init__(self,path): 
        self.Batch_Directory = path
        self.schema_path = 'config/schema_training.json'
        self.logger = App_logger

    def ValuesFromSchema(self):
        """
              This function helps in extracting the values from pre-defined schema file.
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

            file = open("Phishing/Training_Logs/ValuesFromSchemaValidationLog.txt","a+")
            message = "LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberOfColumns + "\n"
            self.logger.log(file,message)

            file.close()

        except ValueError:
            file = open("Phishing/Training_Logs/ValuesFromSchemaValidationLog.txt","a+")
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Phishing/Training_Logs/ValuesFromSchemaValidationLog.txt","a+")
            self.logger.log(file,"KeyError:Key Value Error.Incorrect key is passed.")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Phishing/Training_Logs/ValuesFromSchemaValidationLog.txt","a+")
            self.logger.log(file,str(e))
            file.close()
            raise e

        
    def ManualRegexCreation(self):
        """
            This function is based on regex which helps in validating the filename which is present in  "schema" file.
        """

        regex = "['phishing']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def CreateDirectoryForGoodBadRawData(self):
        """
           This function creates directories for good and bad data after validating training data.
        """

        try:
            path = os.path.join("Phishing/"+"Training_Raw_Files_Validated/"+"Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Phishing/"+"Training_Raw_Files_Validated/"+"Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Phishing/Training_Logs/GeneralLog.txt",'a+')
            self.logger.log(file,"Error while creating Directory. %s:" %ex)
            file.close()
            raise OSError

    def DeleteExistingGoodDataTrainingFolder(self):
        """
            This method helps in deleting the existing folder of good data which is stored in database, so that directory ensures space optimization.
        """

        try:
            if os.path.isdir("Phishing/"+"Training_Raw_Files_Validated/"+"Good_Raw/"):
                shutil.rmtree("Phishing/"+"Training_Raw_Files_Validated/"+"Good_Raw/")
                file = open("Phishing/Training_Logs/GeneralLog.txt",'a+')
                self.logger.log(file,"GoodRaw Directory has been deleted successfully.")
                file.close()
            
        except OSError as s:
            file = open("Phishing/Training_Logs/GeneralLog.txt",'a+')
            self.logger.log(file,"Error while deleting directory. : %s" %s)
            file.close()
            raise OSError

    def DeleteExistingBadDataTrainingFolder(self):
        """
            This method helps in deleting the existing folder of bad data which is stored in database, so that directory ensures 
            space optimization.
        """

        try:
            if os.path.isdir("Phishing/"+"Training_Raw_Files_Validated/"+"Bad_Raw/"):
                shutil.rmtree("Phishing/"+"Training_Raw_Files_Validated/"+"Bad_Raw/")
                file = open("Phishing/Training_Logs/GeneralLog.txt",'a+')
                self.logger.log(file,"BadRaw Directory has been deleted successfully.")
                file.close()
            
        except OSError as s:
            file = open("Phishing/Training_Logs/GeneralLog.txt",'a+')
            self.logger.log(file,"Error while deleting directory. : %s" %s)
            file.close()
            raise OSError

    def MoveBadFilesToArchieveBad(self):
        """
            This method deletes the directory where the bad data is stored after moving the data to archieve folder.
            The archieve bad data will be sent to client for invalid data.
        """

        now = datetime.now()
        date = now.date()
        time = now.stftime("%H%M%S")

        try:
            source = 'Phishing/Training_Raw_Files_Validated/Bad_Raw/'

            if os.path.isdir(source):
                path = "Phishing/TrainingArchieveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'Phishing/TrainingArchiveBadData/BadData_' + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source+f,dest)
                
                file=open("Phishing/Training_Logs/GeneralLog.txt",'a+')
                self.logger.log(file,"Bad files moved to archieve.")
                path = 'Phishing/Training_Raw_Files_Validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder deleted successfully.")
                file.close()

        except Exception as e:
            file = open("Phishing/Training_Logs/GeneralLog.txt",'a+')
            self.logger.log(file,"Error while moving bad date to archieve. : %s" %e)
            file.close()
            raise e

    def ValidationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
            This function validates the name of the training csv files as per given name in the schema.Regex pattern is used to do the
            validation.If name format do not match the file is moved to Bad Raw Data folder else in Good raw data.
        """

        self.DeleteExistingGoodDataTrainingFolder()
        self.DeleteExistingBadDataTrainingFolder()

        onlyfiles = [f for f in listdir(self.Batch_Directory)]

        try:
            self.CreateDirectoryForGoodBadRawData()
            f = open("Phishing/Training_Logs/nameValidationLog.txt", 'a+')

            for filename in onlyfiles:
                if (re.match(regex,filename)):
                    splitAtDot = re.split('.csv',filename)
                    splitAtDot = (re.split('-',splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Phishing/Training_Batch_Files"+filename,"Phishing/Training_Raw_files_validated/Good_Raw")
                            self.logger.log(f,"Valid file name!!File moved to good raw folder :%s" % filename)

                        else:
                            shutil.copy("Phishing/Training_Batch_Files/" + filename, "Phishing/Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("Phishing/Training_Batch_Files/" + filename, "Phishing/Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("Phishing/Training_Batch_Files/" + filename, "TPhishing/raining_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("Phishing/Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName: %s" % e)
            f.close()
            raise e

    def ValidateColumnLength(self,NumberOfColumns):
        """
            This function helps in validating number of columns in csv as same as schema file. If the data is same and nmber of columns 
            are same then data is sent to good raw data folder.Otherwise,bad raw data folder. In csv , if the first column name is 
            missing, then function changes the missing name to "wafer".
        """

        try:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f,"Column Length Validation Started!!")

            for file in listdir("Phishing/Training_Raw_files_validated/Good_Raw/"):
                csv = pd.read_csv("Phishing/Training_Raw_files_validated/Good_Raw/"+file)
                if csv.shape[1] == NumberOfColumns:
                    pass
                else:
                    shutil.move("Phishing/Training_Raw_files_validated/Good_Raw/" + file, "Phishing/Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f,"Column length validation completed.")

        except OSError:
            f = open("Phishing/Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError

        except Exception as e:
            f = open("Phishing/Training_Logs/columnValidationLog.txt",'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e

        f.close()

    def ValidateMissingValuesInWholeColumn(self):
        """
           This function checks whether any column is missing or not. If any column is missing then, the data is sent to bad raw data .
           As, further , the data cannot be processed for preprocessing. 
        """

        try:
            f = open("Phishing/Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Missing Values Validation Started!!")

            for file in listdir('Phishing/Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("Phishing/Training_Raw_files_validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move("Phishing/Training_Raw_files_validated/Good_Raw/" + file,
                                    "Phishing/Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder : %s" % file)
                        break
                if count==0:
                    #csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("Phishing/Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)

        except OSError:
            f = open("Phishing/Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file : %s" % OSError)
            f.close()
            raise OSError

        except Exception as e:
            f = open("Phishing/Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured: %s" % e)
            f.close()
            raise e

