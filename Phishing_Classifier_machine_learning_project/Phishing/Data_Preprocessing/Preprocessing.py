import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn_pandas import CategoricalImputer
from logging_app.logger import App_logger


class Preprocessor:
    """
        This class is used to clean and transform data before training.
    """

    def __init__(self,file_object):
        self.file_object = file_object
        self.logger = App_logger

    def Remove_Columns(self,data,columns):
        """
            This function removes certain columns which are specified from pandas dataframe and gives output as remaining columns. 
        """

        self.logger.log(self.file_object,"Entered the remove_columns method of the Preprocessor class.")
        self.data = data
        self.columns = columns

        try:
            self.useful_data = self.data.drop(labels = self.Columns,axis = 1) ##drop the labels specified in the column
            self.logger.log(self.file_object,'Column removal Successful.Exited the remove_columns method of the Preprocessor class')

            return self.useful_data

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in remove_columns method of the Preprocessor class. Exception message:  '+str(e))
            self.logger.log(self.file_object,'Column removal Unsuccessful. Exited the remove_columns method of the Preprocessor class')

            raise Exception()

    def Seperate_Label_Feature(self,data,Label_Column_Name):
        """
            This function seperates label and feature columns.
        """

        self.logger.log(self.file_object,'Entered the separate_label_feature method of the Preprocessor class')

        try:
            self.x = data.drop(labels = Label_Column_Name,axis=1) ##drop label column and feature columns are present.
            self.y = data[Label_Column_Name] ##Label column
            self.logger.log(self.file_object,'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class.')

            return self.x,self.y

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger.log(self.file_object, 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise Exception()

    def DropUnnecessaryColumns(self,data,ColumnNameList):
        """
            This function drops unwanted columns 
        """

        data = data.drop(ColumnNameList,axis=1)
        return data

    def ReplaceInvalidValuesWithNull(self,data):
        """
            This method replaces invalid values i.e. '?' with null.
        """

        for column in data.columns:
            count = data[column][data[column] == '?'].count()
            if count!=0:
                data[column] = data[column].replace('?',np.nan)
        return data

    def IsNullPresent(self,data):
        """
            This function checks the null values.
        """

        self.logger.log(self.file_object,'Entered the is_null_present method of the Preprocessor class')
        self.null_present = False
        self.cols_with_missing_values=[]
        self.cols = data.columns

        try:
            self.null_counts = data.isna().sum()  ##check null values count for column
            for i in range(len(self.null_counts)):
                if self.null_counts[i]>0:
                    self.null_present = True
                    self.cols_with_missing_values.append(self.cols[i])
            
            if(self.null_present):  ##write logs to see which columns have null values
                self.dataframe_with_null = pd.DataFrame()
                self.dataframe_with_null['columns'] = data.columns
                self.dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                self.dataframe_with_null.to_csv('Phishing/Preprocessing_Data/null_values.csv') # storing the null column information to file
            self.logger.log(self.file_object,'Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return self.null_present, self.cols_with_missing_values

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in is_null_present method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger.log(self.file_object,'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise Exception()

    def EncodeCategoricalValues(self,data):
        """
            This function encodes all the categorical values in the training data.
        """

        data["class"] = data["class"].map({'p': 1, 'e': 2})

        for column in data.drop(['class'],axis=1).columns:
            data = pd.get_dummies(data, columns=[column])

        return data

    def Impute_Missing_Values(self, data, cols_with_missing_values):
        """
            This method replaces all the missing values in the Dataframe using KNN Imputer.
        """
        self.logger.log(self.file_object, 'Entered the impute_missing_values method of the Preprocessor class')
        self.data= data
        self.cols_with_missing_values=cols_with_missing_values

        try:
            self.imputer = CategoricalImputer()
            for col in self.cols_with_missing_values:
                self.data[col] = self.imputer.fit_transform(self.data[col])
            self.logger.log(self.file_object, 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
            return self.data

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in impute_missing_values method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger.log(self.file_object,'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
            raise Exception()

    def Get_Columns_With_Zero_Std_Deviation(self,data):
        """
            This function finds the columns which has standard deviation zero.
        """

        self.logger.log(self.file_object, 'Entered the get_columns_with_zero_std_deviation method of the Preprocessor class')
        self.columns=data.columns
        self.data_n = data.describe()
        self.col_to_drop=[]
        try:
            for x in self.columns:
                if (self.data_n[x]['std'] == 0): # check if standard deviation is zero
                    self.col_to_drop.append(x)  # prepare the list of columns with standard deviation zero
            self.logger.log(self.file_object, 'Column search for Standard Deviation of Zero Successful. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            return self.col_to_drop

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in get_columns_with_zero_std_deviation method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger.log(self.file_object, 'Column search for Standard Deviation of Zero Failed. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            raise Exception()

