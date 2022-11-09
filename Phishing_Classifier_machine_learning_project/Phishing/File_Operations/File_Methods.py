import pickle
import os
import shutil
from logging_app.logger import App_logger

class File_Operation:
    """
        This class is used to save the model after training.
    """

    def __init__(self,file_object):
        self.file_object = file_object
        self.logger = App_logger
        self.model_directory = "Phishing/Models/"

    def Save_Model(self,model,filename):
        """
            This function saves the model.
        """

        self.logger.log(self.file_object,'Entered save_model method of the file_operation class.')

        try:
            path = os.join.path(self.model_directory,filename)  ##creates seperate directory for each cluster

            if os.path.isdir(path):
                shutil.rmtree(self.model_directory)
                os.makedirs(path)

            else:
                os.makedirs(path)

            with open(path + '/' + filename + '.sav', 'wb') as f:
                pickle.dump(model, f) # save the model to file

            self.logger.log(self.file_object,'Model_File',filename + 'saved. Exited the save_model method of the Model_Finder class')

            return 'success.'

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in save_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger.log(self.file_object,'Model File '+filename+' could not be saved. Exited the save_model method of the Model_Finder class')
            raise Exception()

    def Load_Model(self,filename):
        """
            This function will load the model.
        """

        self.logger.log(self.file_object, 'Entered the load_model method of the File_Operation class')

        try:
            with open(self.model_directory + filename + '/' + filename + '.sav','rb') as f:
                self.logger.log(self.file_object,'Model File ' + filename + ' loaded. Exited the load_model method of the Model_Finder class')
                return pickle.load(f)

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in load_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger.log(self.file_object,'Model File ' + filename + ' could not be saved. Exited the load_model method of the Model_Finder class')
            raise Exception()

    def Find_Correct_Model_File(self,cluster_number):
        """
            Selecing the correct model based on cluster number.
        """
        self.logger.log(self.file_object, 'Entered the find_correct_model_file method of the File_Operation class')

        try:
            self.cluster_number= cluster_number
            self.folder_name=self.model_directory
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)

            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str( self.cluster_number))!=-1):
                        self.model_name=self.file
                except:
                    continue
            self.model_name=self.model_name.split('.')[0]
            self.logger.log(self.file_object,'Exited the find_correct_model_file method of the Model_Finder class.')
            return self.model_name

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in find_correct_model_file method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger.log(self.file_object,'Exited the find_correct_model_file method of the Model_Finder class with Failure')
            raise Exception()




