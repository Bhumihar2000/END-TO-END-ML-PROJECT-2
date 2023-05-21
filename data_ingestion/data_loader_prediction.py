import pandas as pd

class Data_Getter_Pred:
    """
            This class shall be usedd for obtaining the data from the source for prediction.

            Written by:- Sachin Bhumihar
            Version:- 1.0
            Revision:- None
    """
    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.prediction_file = 'Prediction_FileFromDB/InputFile.csv'

    def get_data(self):
        """
                Method Name:- get_data
                Description:- This method reads the data from source.
                Output:- A Pandas Dataframe
                On Failure:- Raise Exception

                Written by:- Sachin Bhumihar
                Version:- 1.0
                Revision:- None
        """    
        self.logger_object.log(self.file_object, "Entered the get_data method of the Data_Getter class")
        try:
            self.data = pd.read_csv(self.prediction_file)# reading the data file
            self.logger_object.log(self.file_object, "Data Load Successful. Exited the get_data method of the Data_Getter class")
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object, "Exception Occured in get_data method of the Data_Getter class. Exception Message: "+str(e))
            self.logger_object.log(self.file_object, "Data Load Unsuccessful. Exited the get_data method of the Data_Getter class")
            raise Exception()
        