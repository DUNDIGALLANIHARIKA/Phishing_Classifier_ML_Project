import pandas as pd
from logging_app.logger import App_logger

class Data_Getter:
    """
        This class is used for obtaining data from the source for training.
    """

    def __init__(self,file_object,logger):
        self.file_object = file_object
        self.logger = App_logger
        self.training_file = 'Phishing/Training_FileFromDB/InputFile.csv'

    def Get_Data(self):
        """
            This function reads the data from source.
        """

        self.logger.log(self.file_object,'Entered the get_data method of the Data_Getter class')

        try:
            self.data= pd.read_csv(self.training_file) # reading the data file
            self.logger.log(self.file_object,'Data Load Successful.Exited the get_data method of the Data_Getter class')
            return self.data

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in get_data method of the Data_Getter class. Exception message: '+str(e))
            self.logger.log(self.file_object,'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise Exception()