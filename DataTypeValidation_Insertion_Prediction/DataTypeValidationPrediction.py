import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
from application_logging.logger import App_Logger

class dBOperation:
    """
            This class shall be used for handling all the SQL operations.

            Written By:- Sachin Bhumihar
            Version:- 1.0
            Revision:- None
    """
    def __init__(self):
        self.path = 'Prediction_Database/'
        self.badFilePath = "Prediction_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()

    def dataBaseConnection(self, DatabaseName):
        """
                Method Name:- dataBaseConnection
                Description:- This Method creates the database with the given name and if Database already exists then opens the connection to the DB.
                Output:- Connection to the DB
                On Failure:- Raise ConnectionError

                Written By:- Sachin Bhumihar
                Version:- 1.0
                Revision:- None
        """    
        try:
            conn = sqlite3.connect(self.path+DatabaseName+'.db')

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Opened %s database successfully" % DatabaseName)
            file.close()
        except ConnectionError:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database : %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return conn
    def createTableDb(self, DatabaseName, column_names):
        """
                    Method Name:- createTableDb
                    Description:- This method creates a table in the given database which will be used to inserrt the Good data after raw data validation.
                    Output:- None
                    On Failure:- Raise Exception

                    Written By:- Sachin Bhumihar
                    Version:- 1.0
                    Revision:- None
        """    
        try:
            conn = self.dataBaseConnection(DatabaseName)
            conn.execute('DROP TABLE IF EXISTS Good_Raw_Data;')

            for key in column_names.keys():
                type = column_names[key]

                # Wee will remove the column of string datatype before loading as it is not needed for training
                # In try block we check if the table exists, if yes then add columns to the table
                # else in catch block we create this table
                try:
                    conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name = key, dataType = type))
                except:
                    conn.execute('CREATE TABLE Good_Raw_Data ({column_name} {dataType})'.format(column_name = key, dataType = type))

            conn.close()

            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Table created successfully!!")
            file.close()

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()

        except Exception as e:
            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error While creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully " % DatabaseName)
            file.close()
            raise e


    def insertIntoTableGoodData(self, Database):
        """
                    Method Name:- insertIntoTableGoodData
                    Description:- This Method inserts the Good data files from the Good_Raw folder into the above created table.
                    Output:- None
                    On Failure:- Raise Exception

                    Written By:- Sachin Bhumihar
                    Version:- 1.0
                    Revision:- None
        """                    

        conn = self.dataBaseConnection(Database)
        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Prediction_Logs/DbInsertLog.txt", 'a+')

        for file in onlyfiles:
            try:
                with open(goodFilePath+'/'+file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values = (list_)))
                                self.logger.log(log_file," %s: File Loaded successfully !!" % file)
                                conn.commit()

                            except Exception as e:
                                raise e
            except Exception as e:
                conn.rollback()
                self.logger.log(log_file, "Error While creating table: %s" % e)
                shutil.move(goodFilePath+'/' + file, badFilePath)
                self.logger.log(log_file, "File Moved Successfully %s " % file)
                log_file.close()
                conn.close()
                raise e

        conn.close()
        log_file.close()

    def selectingDatafromtableintocsv(self, Database):
        """
                    Method Name:- selectingDatafromtableintocsv
                    Description:- This method exports the data in GoodData table as a CSV file. in a given location.
                                    above created.
                    Output:- None
                    On Failure:- Raise Exception

                    Written By:- Sachin Bhumihar
                    Version:- 1.0
                    Revision:- None                
        """                        

        self.fileFromDb = "Prediction_FileFromDB/"
        self.fileName = 'InputFile.csv'
        log_file = open("Prediction_Logs/ExportToCsv.txt", 'a+')
        try:
            conn = self.dataBaseConnection(Database)
            sqlSelect = "SELECT * FROM Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sqlSelect)

            results = cursor.fetchall()

            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            # Make the csv output directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open csv file for writing
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''), delimiter=',', lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the csv file.
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File Exported successfully!!!")

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error: %s" %e)
            raise e            
