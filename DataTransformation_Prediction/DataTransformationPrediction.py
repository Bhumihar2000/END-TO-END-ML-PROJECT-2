from datetime import datetime
from os import listdir
import pandas
from application_logging.logger import App_Logger

class dataTransformPredict:
    """
            This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

            Written By:- Sachin Bhumihar
            Version:- 1.0
            Revision: None
    """
    def __init__(self):
        self.goodDataPath = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()

    def replaceMissingWithNull(self):
        """
                    Method Name:- replaceMissingWithNull
                    Description:- This Method replaces the missing values in coliumnm with "NULL" to store in the table.
                                    We are using substring in the first column to keep only "Integer" data for ease ip the loading.
                                    This column is anyway going to be removed during prediction.

                    Written By:- Sachin Bhumihar
                    Version:- 1.0
                    Revision:- None

        """    
        try:
            log_file = open("Prediction_Logs/dataTransformLog.txt", 'a+')
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                csv = pandas.read_csv(self.goodDataPath+"/"+file)
                csv.fillna('NULL', inplace=True)
                # csv.update("'"+csv['Wafer'] +"'")
                # csv.update(csv['Wafer'].astype(str))
                csv['Wafer'] = csv['Wafer'].str[6:]
                csv.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                self.logger.log(log_file, " %s: File Transformed successfully !!" % file)

        except Exception as e:
            self.logger.log(log_file, "Data Transformation Failed because:: %s" % e)
            log_file.close()
            raise e
        log_file.close()
                

