from datetime import date, datetime
from os import listdir
from logging_app.logger import App_logger
import pandas as pd

class DataTransform:
    """
        This class is used in transformation good raw training data before sending it to  database. 
    """

    def __init__(self):
        self.GoodDataPath = "Phishing/Training_Raw_files_validated/Good_Raw"
        self.logger = App_logger

    def AddQuotesToStringValuesInColumn(self):
        """
            This method converts all the string value data type in the column is enclosed in quotes , to avoid the error while 
            inserting string values in table as varchar.
        """

        log_file = open("Phishing/Training_Logs/addQuotesToStringValuesInColumn.txt", 'a+')

        try:
            onlyfiles = [f for f in listdir(self.GoodDataPath)]
            for file in onlyfiles:
                data = pd.read_csv(self.GoodDataPath+"/"+file)

                for column in data.columns:
                    count = data[column][data[column] == '?'].count()
                    if count != 0:
                        data[column] = data[column].replace('?', "'?'")
                data.to_csv(self.GoodDataPath+ "/" + file, index=None, header=True)
                self.logger.log(log_file," %s: Quotes added successfully!!" % file)

        except Exception as e:
            self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
            log_file.write("Data Transformation failed because:: %s" % e + "\n")
            log_file.close()

        log_file.close()

        

