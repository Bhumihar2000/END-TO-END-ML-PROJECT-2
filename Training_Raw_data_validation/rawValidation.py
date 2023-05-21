import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger


class Raw_Data_validation:
    """


        This class shall be used for handling all the validation done on the raw training data!!.

        Written By:- Sachin Bhumihar
        Version:- 1.0    
        Revision:- None
    
    
    
    """

    def __init__(self,path):
        self.Bath_Directory = path
        self.schema_path = "schema_training.json"
        self.logger = App_Logger()


    def valuesFromSchema(self):
        """
            Method Name:- valuesFromSchema
            Description:- This Method extracts all the relevant information from the pre=defined "schema" file.
            output:- LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number_of_columns
            on Failure:- Raise ValueError, KeyError, Exception

            Written by:- Sachin Bhumihar
            Version:- 1.0
            Revision:- None
        
        """    

        try:
            with open(self.schema_path, "r") as f:
                dic = json.load(f)
                f.close()

            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile'] 
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", "a+")
            message = "LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" %LengthOfTimeStampInFile + "\t" + "NumberofColumns:: %s" %NumberofColumns + "\n"
            self.logger.log(file, message)   

        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "ValueError: Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError : Key Value Error incorrect key passed")
            file.close()
            raise KeyError
        
        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e
        

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns
    

    def manualRegexCreation(self):
        """
            Method Name:- manualRegexCreation
            Description:- This Methid contains a manually defined regex based on the "filename" given in "Schema" file.
                            This Regex is used to validate the file name of the training data
            output:- Regex Pattern
            on failure:- None

            Written By:- Sachin Bhumihar
            Version :- 1.0
            Revision:- None

        """

        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex
    

    def createDirectoryForGoodBadRawData(self):
        """
                Method Name:- createDirectoryForGoodBadRawData
                Description:- This Method creates directories to store the good data and bad data after validating the training data.
                output:- None
                On Failure:- OSError

                Written By:- Sachin Bhumihar
                Version:- 1.0
                Revision :- None

        """


        try: 
            path = os.path.join("Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
                
        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", "a+")
            self.logger.log(file, "Error while creating Directory %s:" % ex)
            file.close()
            raise OSError



    def deleteExistingGoodDataTrainingFolder(self):
        """
            Method Name:= deleteExistingGoodDataTrainingFolder
            description:- This method deletes the directory made to store the Good data after loading the data in table.
                            Once the good files are loaded in the DB, deleting thed directory ensures space optimization.
            output:- None
            On Failure:- Os ERROr

            Written Buy:- Sachin Bhumihar
            version:- 1.0
            Revision :- None

        """ 

        try:
            path = "Training_Raw_files_validated/"
            # if os.path.isdir("ids/" + userName):
            # if os.path.join(path + 'Bad_Raw/'):
            #       shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "GoodRaw directory deleted successfully!!!")
                file.close()

        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError



    def deleteExistingBadDataTrainingFolder(self):
        """
                Method Name:- deleteExistingBadDataTrainingFolder
                Description:- This method delete the directory made to store the bad adata.
                output:- None
                On Failure:- OSError

                Written By:- Sachin Bhumihar
                Version:- 1.0
                Revision:- None

        """     

        try:
            path = "Training_Raw_files_validated/"
            if os.path.isdir(path + 'Bad_Raw'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "BadRaw directory deleted before starting validation!!!")
                file.close()

        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError
        
    def moveBadFilesToArchiveBad(self):
        """
                Method Name:- moveBadFilesToArchiveBad
                Description:- Thos Method deletes the directory made to store the Bad Data after moving the data in an archive folder.
                                we archive the bad filed to send them back to the client for invalid data issue.
                Output :- None
                On Failure:- OSError

                Written By:- Sachin Bhumihar
                Version:- 1.0
                Revision:- None                
        """

        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source = 'Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)

                dest = 'TrainingArchiveBadData/BadData_' + str(date) + "_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Bad files moved to archive")
                path = "Training_Raw_files_validated/"
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file, "Bad Raw Data Folder deleted successfully !!")
                file.close()

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error While moving bad files tro archive :: %s" % e)
            file.close()
            raise e


    def validationFileNameRaw(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile):
        """
                Method Name:- validationFileNameRaw
                Description:- This Function Validates the name of the training csv files as per given name in the schema!
                                Regex pattern is used to do the validatrion. if name format do not match the file is moved to Bad Raw Data folder else in Good Raw data.
                output:- None
                On Failure:- Exception

                Writtten By:- Sachin Bhumihar
                Version:- 1.0
                Revision:- None                
        """

        # pattern  = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        # delete the directories for good and bad data in case last run was unsuccessful and folder were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        #Create new directories
        self.createDirectoryForGoodBadRawData()
        onlyfiles = [f for f in listdir(self.Bath_Directory)]
        try:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if(re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")
                            self.logger.log(f, "Valid File Name!! File Moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f, "Invalid File Name!! File Moved to Bad Raw Folder :: %s" % filename)
                    else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f, "Invalid File Name!! File Moved to Bad Raw Folder :: %s" % filename)                                                                   
                else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f, "Invalid File Name!! File Moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while validating File Name %s" % e)
            f.close()
            raise e


    def validateColumnLength(self, NumberofColumns):
        """
                Method Name:- validateColumnLength
                Description:- This Function validates the number of column in the csv files.
                                it is should be same as giben in the schema files.
                                if not sa,me file is not suitable for processing and thus is moved to Bad Raw Data Foleder.
                                If the column number matches, file is kept in Good Raw Data For Processing.
                                The CSV file missing the first column name this functioon changes the misiing name to "Wafer".
                output:- Npne
                On Failure:- Exception

                Written By:- Sachin Bhumihar
                Version:- 1.0
                Revision:- None                
        """
        try:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Column Length Validation Started!!")
            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" +file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move("Training_Raw_files_validate/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file !! File Moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")

        except OSError:
            f = open("Training_Logs/ColumnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured :: %s" % e)
            f.close()
            raise e
        f.close()
        
    def validateMissingValuesInWholeColumn(self):
        """
                Method Name:- validateMissingValuesInWholeColumn
                Description:- Thsi Function validates if any column in the csv filwe has all values missing.
                                if all the values are missing, the file is not suitable for processing.
                                Such files are moved to bad raw data.

                output:- None
                On Failure:- Exception

                Written By:- Sachin bhumihar
                Version:- 1.0
                Revision:- None

        """

        try:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started")

            for file in listdir("Training_Raw_files_validated/Good_Raw/"):
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns])-csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f, "Invalid Column Length for the file!! File Moved to Bad Raw Folder :: %s" % file)
                        break
                if count == 0:
                    csv.rename(columns = {"Unnamed: 0": "Wafer"}, inplace = True)
                    csv.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index = None, header = True)

        except OSError:
            f = open("Training_Logs/missingValueInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while Moving the fike :: %s"  % OSError)
            f.close()
            raise OSError

        except Exception as e:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured :: %s" % e)
            f.close()
            raise e
        f.close()
                                                