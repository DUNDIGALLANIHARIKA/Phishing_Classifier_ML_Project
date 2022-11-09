import pandas as pd
from logging_app.logger import App_logger

class Data_Getter_Pred:
    """
        This class shall  be used for obtaining the data from the source for prediction.
    """

    def __init__(self, file_object):
        self.prediction_file='Phishing/Prediction_FileFromDB/InputFile.csv'
        self.file_object=file_object
        self.logger = App_logger

    def Get_Data(self):
        """
            This function reads the data from source.
        """

        self.logger.log(self.file_object,'Entered the get_data method of the Data_Getter class')

        try:
            self.data= pd.read_csv(self.prediction_file) # reading the data file
            self.logger.log(self.file_object,'Data Load Successful.Exited the get_data method of the Data_Getter class')
            return self.data

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in get_data method of the Data_Getter class. Exception message: '+str(e))
            self.logger.log(self.file_object,'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise Exception()


