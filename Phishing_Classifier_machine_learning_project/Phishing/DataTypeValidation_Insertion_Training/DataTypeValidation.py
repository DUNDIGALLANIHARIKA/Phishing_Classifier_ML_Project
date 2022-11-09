import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
from logging_app.logger import App_logger

class DBOperation:
    """
        This class is used for handling SQL operations.
    """

    def __init__(self):
        self.path = "Phishing/Training_Database/"
        self.BadFilePath = "Phishing/Training_Raw_Files_Validated/Bad_Raw"
        self.GoodFilePath = "Phishing/Training_Raw_Files_Validated/Good_Raw"
        self.logger = App_logger

    def DataBaseConnection(self,DataBaseName):
        """
            This function creates a database. If the database already exists, then it connects to database.
        """

        try:
            conn = sqlite3.connect(self.path+DataBaseName+'.db')

            file = open("Phishing/Training_Logs/DataBaseConnectionLog.txt",'a+')
            self.logger.log(file,"Opened %s database successfully." %DataBaseName)
            file.close()

        except ConnectionError:
            file = open("Phishing/Training_Logs/DataBaseConnectionLog.txt",'a+')
            self.logger.log(file,"Error while connecting to database." %ConnectionError)
            file.close()
            raise ConnectionError

        return conn

    def CreateTableDB(self,DataBaseName,column_names):
        """
            This function creates a table in database where we insert good data after raw data validation.
        """

        try:
            conn = self.DataBaseConnection(DataBaseName)
            c = conn.cursor()
            c.execute("Select count(name) From sqlite_master WHERE type = 'table' AND NAME = 'Good_Raw_Data'")
            if c.fetchone()[0] == 1:
                conn.close()
            
                file = open("Phishing/Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()

                file = open("phishing/Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % DataBaseName)
                file.close()

            else:
                for key in column_names.keys():
                    type = column_names[key]

                    ##In try block,we check whether the table exists or not. If the table exists, then we add columns to the table.
                    ##If there is no table, then it enters into except block and creates the table.

                    try:
                        conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                    except:
                        conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))

                conn.close()

                file = open("Phishing/Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()

                file = open("Phishing/Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % DataBaseName)
                file.close()

        except Exception as e:
            file = open("Phishing/Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Phishing/Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DataBaseName)
            file.close()
            raise e

    def InsertGoodDataIntoTable(self,DataBase):
        """
            This function inserts good data files into table from good_raw folder.
        """

        conn = self.DataBaseConnection(DataBase)
        GoodFilePath = self.GoodFilePath
        BadFilePath = self.BadFilePath
        onlyfiles = [f for f in listdir(GoodFilePath)]
        log_file = open("Phishng/Training_Logs/DbInsertLog.txt", 'a+')

        for file in onlyfiles:
            try:
                with open(GoodFilePath+'/'+file,"r") as f:
                    next(f)
                    reader = csv.reader(f,delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in line[1]:
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

        conn.close()
        log_file.close()

    def SelectingDataFromTableIntoCSV(self,DataBase):
        """
            This method exports the data from GoodData table as a CSV file.
        """

        self.FileFromDb = 'Phishing/Training_FileFromDB/'
        self.FileName = 'InputFile.csv'
        log_file = open("Phishing/Training_Logs/ExportToCsv.txt", 'a+')

        try:
            conn = self.DataBaseConnection(DataBase)

            sqlSelect = "Select * FROM Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sqlSelect)

            results = cursor.fetchall()

            ##Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            ##make csv output directory
            if not os.path.isdir(self.FileFromDb):
                os.makedirs(self.FileFromDb)

            ##open csv file for writing
            csvFile = csv.writer(open(self.FileFromDb + self.FileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\') 
            
            ##add header and data to csv file
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            log_file.close()

                            

 