"""
        This is the Entry point for training the machine learning model
        
        Written By:- Sachin Bhumihar
        Version:- 1.0
        Revision:- None

"""

# Doing the necessary imports
from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from data_preprocessing import preprocessing
from data_preprocessing import clustering
from best_model_finder import tuner
from file_operations import file_methods
from application_logging import logger

# Creating Common Logging Object

class trainModel:

    def __init__(self):
        self.log_writer = logger.App_Logger()
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')


    def trainModel(self):
        # Logging the start of Training
        self.log_writer.log(self.file_object, "Start Of Training")
        try:
            # Getting the data from the source
            data_getter = data_loader.Data_Getter(self.file_object, self.log_writer)
            data = data_getter.get_data()

            """Doing The Data Preprocessing"""

            preprocessor = preprocessing.Preprocessor(self.file_object, self.log_writer)
            data = preprocessor.remove_columns(data, ['Wafer']) # remove the unnamed columns as it does't contribute to prediction.

            # Create separate features and labels
            X,Y = preprocessor.separate_label_feature(data, label_column_name = 'Output')

            # Check if missing values are present in the dataset
            is_null_present = preprocessor.is_null_present(X)

            # if missing values are there, replace them appropriately.
            if(is_null_present):
                X = preprocessor.impute_missing_values(X) # Missing Value Imputation

            # Check further which columns do not contribute to predictions
            # if the standard deviation for a column is zero, it means that the column has constant values
            # and they are giving the same output both for good and bad sensors
            # prepare the list of such columns to drop

            cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(X)


            # drop the columns obtained above
            X = preprocessor.remove_columns(X, cols_to_drop)

            """Applying the clustering approach"""

            kmeans = clustering.KmeanClustering(self.file_object, self.log_writer) # Object Initialization
            number_of_clusters = kmeans.elbow_plot(X) # using the elbow plot to find the number of optimum clusters

            # Divide the data into clusters
            X = kmeans.create_clusters(X, number_of_clusters)

            # Create a new column in the dataset consisting of the corresponding cluster assignments.
            X['labels'] = Y

            # getting the unique clusters from our dataset
            list_of_clusters = X['Cluster'].unique()

            """Parsing all the clusters and looking for the best ML Algorithm to fit on individual cluster"""
            for i in list_of_clusters:
                cluster_data = X[X['Cluster'] == i] # filter the data for one cluster

                #preapre the feature and label columns
                cluster_features =  cluster_data.drop(['Labels', 'Cluster'], axis = 1)
                cluster_label = cluster_data['Labels']

                # Splitting the data into training and test set for each cluster one by one

                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1/3, random_state=355)

                model_finder = tuner.Model_Finder(self.file_object, self.log_writer) # Object initialization

                # Getting the best model for each of the cluster
                best_model_name, best_model = model_finder.get_best_model(x_train, y_train, x_test, y_test)

                # Saving the best model to the directory.
                file_op = file_methods.File_Operation(self.file_object, self.log_writer)
                save_model = file_op.save_model(best_model, best_model_name+str(i))

            # logging the successful Training
            self.log_writer.log(self.file_object, "Successful End of Training")
            self.file_object.close()

        except Exception:
            # logging the unsuccessful Training.
            self.log_writer.log(self.file_object, "Unsuccessful End of Trainign")
            self.file_object.close()
            raise Exception
                




            
                    