from datetime import datetime
from os import listdir
import pandas
from logging_app.logger import App_logger

class DataTransformPredict:
    """
        This class is used in transformation good raw training data before sending it to  database. 
    """
    
    def __init__(self):
          self.GoodDataPath = "Phishing/Prediction_Raw_Files_Validated/Good_Raw"
          self.logger = App_logger


    def AddQuotesToStringValuesInColumn(self):
        """
            This method converts all the string value data type in the column is enclosed in quotes , to avoid the error while 
            inserting string values in table as varchar.
        """
          
        try:
               log_file = open("Phishing/Prediction_Logs/DataTransformLog.txt", 'a+')
               onlyfiles = [f for f in listdir(self.GoodDataPath)]
               for file in onlyfiles:
                    data = pandas.read_csv(self.GoodDataPath + "/" + file)
                    for column in data.columns:
                         count = data[column][data[column] == '?'].count()
                         if count != 0:
                              data[column] = data[column].replace('?', "'?'")
                    data.to_csv(self.GoodDataPath + "/" + file, index=None, header=True)
                    self.logger.log(log_file, " %s: Quotes added successfully!!" % file)

        except Exception as e:
               log_file = open("Phishing/Prediction_Logs/DataTransformLog.txt", 'a+')
               self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
               log_file.close()
               raise e

        log_file.close()
