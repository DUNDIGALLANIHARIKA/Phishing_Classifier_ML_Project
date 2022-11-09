import shutil
import sqlite3
from os import listdir
import os
import csv
from logging_app.logger import App_logger

class DBOperation:
    """
        This class is used for handling SQL operations
    """

    def __init__(self):
        self.path = 'Phishing/Prediction_Database/'
        self.BadFilePath = "Phishing/Prediction_Raw_Files_Validated/Bad_Raw"
        self.GoodFilePath = "Phishing/Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_logger


    def DataBaseConnection(self,DataBaseName):
        """
            This function creates a database. If the database already exists, then it connects to database.           
        """
        try:
            conn = sqlite3.connect(self.path+DataBaseName+'.db')

            file = open("Phishing/Prediction_Logs/DBConnectionLog.txt", 'a+')
            self.logger.log(file, "Opened %s database successfully" % DataBaseName)
            file.close()

        except ConnectionError:
            file = open("Prediction_Logs/DBConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError

        return conn

    def CreateTableDB(self,DataBaseName,column_names):
        """
           This function creates a table in database where we insert good data after raw data validation.
        """

        try:
            conn = self.DataBaseConnection(DataBaseName)
            conn.execute('DROP TABLE IF EXISTS Good_Raw_Data;')

            for key in column_names.keys():
                type = column_names[key]

                # we will remove the column of string datatype before loading as it is not needed for training
                #in try block we check if the table exists, if yes then add columns to the table
                # else in catch block we create the table
                try:
                    #cur = cur.execute("SELECT name FROM {dbName} WHERE type='table' AND name='Good_Raw_Data'".format(dbName=DatabaseName))
                    conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                except:
                    conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))

            conn.close()

            file = open("Phishing/Prediction_Logs/DBTableCreateLog.txt", 'a+')
            self.logger.log(file, "Tables created successfully!!")
            file.close()

            file = open("Phishing/Prediction_Logs/DBConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DataBaseName)
            file.close()

        except Exception as e:
            file = open("Phishing/Prediction_Logs/DBTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()

            file = open("Phishing/Prediction_Logs/DBConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DataBaseName)
            file.close()
            raise e

    def InsertIntoTableGoodData(self,DataBase):
        """
            This function inserts good data files into table from good_raw folder.
        """
        
        conn = self.DataBaseConnection(DataBase)
        GoodFilePath= self.GoodFilePath
        BadFilePath = self.BadFilePath
        onlyfiles = [f for f in listdir(GoodFilePath)]
        log_file = open("Phishing/Prediction_Logs/DBInsertLog.txt", 'a+')

        for file in onlyfiles:
            try:

                with open(GoodFilePath+'/'+file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_)))
                                self.logger.log(log_file," %s: File loaded successfully!!" % file)
                                conn.commit()
                            except Exception as e:
                                raise e

            except Exception as e:

                conn.rollback()
                self.logger.log(log_file,"Error while creating table: %s " % e)
                shutil.move(GoodFilePath+'/' + file, BadFilePath)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()
                conn.close()
                raise e

        conn.close()
        log_file.close()


    def SelectingDataFromtableIntoCSV(self,DataBase):
        """
            This method exports the data from GoodData table as a CSV file.
        """

        self.FileFromDb = 'Phishing/Prediction_FileFromDB/'
        self.FileName = 'InputFile.csv'
        log_file = open("Phishing/Prediction_Logs/ExportToCSV.txt", 'a+')

        try:
            conn = self.DataBaseConnection(DataBase)
            sqlSelect = "SELECT *  FROM Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sqlSelect)

            results = cursor.fetchall()

            #Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            #Make the CSV ouput directory
            if not os.path.isdir(self.FileFromDb):
                os.makedirs(self.FileFromDb)

            # Open CSV file for writing.
            csvFile = csv.writer(open(self.FileFromDb + self.FileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            raise e

