from datetime import datetime
from os import listdir
import pandas
from application_logging import logger

class dataTransform:
    """
        This class shalll be used for transforming the good raw training data before loading it in database!!.

        Wrritten By:- Sachin Bhumihar
        version:- 1.0
        Revisions:- None
    
    
    
    """

    def __init__(self):
        self.goodDataPath = "Training_Raw_files_validated/Good_Raw"
        self.logger = App_Logger()

    def replaceMissingWithNull(self):
        """
                Method Name:- replaceMissingfWithNull
                Description:- This method replaces the missing values in column with "Null" to store in the table. We are using substring in the first column to keep omly "Integer" data for
                                ease up the loading.
                                This Column is anyway going to be removed during training.

                Written By:- Sachin Bhumihar
                Version:- 1.0
                Revision:- None                
        
        
        
        """    
        log_file = open("Training_Logs/dataTransformLog.txt", 'a+')
        try:
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                csv = pandas.read_csv(self.goodDataPath+"/" + file)
                csv.fillna('NULL',inplace=True)
                # csv.update("'"+ csv['wafer'] + "'")
                # csv.update(csv['wafer'].astype(str))
                csv['Wafer'] = csv['Wafer'].str[6:]
                csv.to_csv(self.goodDataPath+ "/" + file, index = None, header = True)
                self.logger.log(log_file, " %s: File Transformed successfully!!" % file)
                #log_file.write("Current Date : : %s" %date +"\t" + "Current time :: %s" % current_time + "\t \t" + + "\n")

        except Exception as e:
            self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
            #log_file.write("Current Date :: %s" %date +"\t" + "Current time :: %s" % current_time + "\t \t" + "Data Transformation failed because :: %s" % e + "\n")
            log_file.close()
        log_file.close()
        
                    