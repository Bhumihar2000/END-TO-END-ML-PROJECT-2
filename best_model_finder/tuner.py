from sklearn.ensemble import RandomForestClassifier 
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, accuracy_score

class Model_Finder:
    """
            This class shall be used to find the model woith best accuracy and AUC score.
            Written By:- Sachin Bhumihar
            Version:- 1.0
            Revision:- None

    """
    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.clf = RandomForestClassifier()
        self.xgb = XGBClassifier()

    def get_best_params_for_random_forest(self, train_x, train_y):
        """
                Method Name = get_best_params_for_random_forest
                Description:- get the parameters for Random Forest Algorithm which give the best accuracy.
                                Use Hyper parameter Tuning.
                Output:- The Model with the best parameters
                On Failure:- Raise Exception

                Written By:- Sachin Bhumihar
                Version:- 1.0
                Revision:- None

        """    
        self.logger_object.log(self.file_object, "Entered the get_best_params_for_random_forest method of the Model_Finder class")
        try:
            # Initializing with different combination of parameters
            self.param_grid = {"n_estimators": [10,50,100,130], "criterion": ['gini', 'entropy'], "max_depth": range(2,4,1), "max_features" : ['auto', 'log2']}

            # Creating an object of the Grid Search class
            self.grid = GridSearchCV(estimator=self.clf, param_grid=self.param_grid, cv = 5, verbose = 3)
            # finding the best parameters
            self.grid.fit(train_x, train_y)
            
            #extracting the best parameters
            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']
            self.max_features = self.grid.best_params_['max_features']
            self.n_estimators = self.grid.best_params_['n_estimators']

            #Creating a new model with best Parameters
            self.clf = RandomForestClassifier(n_estimators=self.n_estimators, criterion=self.criterion, max_depth=self.max_depth, max_features=self.max_features)

            #training the new model
            self.clf.fit(train_x, train_y)
            self.logger_object.log(self.file_object, "Random Forest Best Params: '+str(Self.grid.best_params_)+' . Exited the get_best_params_for_random_forest method of the Model_Finder class")
            return self.clf
        
        except Exception as e:
            self.logger_object.log(self.file_object, "Exception Occured in get_best_params_for_random_forest method of the Model_finder class. Exception message: " +str(e))
            self.logger_object.log(self.file_object, "Random Forest Parameter tuning Failed. Exited the get_best_params_for_random_forest method of the Model_Finder class")

            raise Exception()
        
    def get_best_params_for_xgboost(self, train_x, train_y):
        """
                Method Name:- get_best_params_for_xgboost
                Description:- get the parameter for XGBoost Alagorithm which give the best accuracy.
                                use hyper parametr tuning
                Output:- The model with best parameters
                On Failure:- Raise Exception

                Written By:- Sachin Bhumihar
                Version:- 1.0
                Revision:- None                
        """    
        self.logger_object.log(self.file_object, "Entered the get_best_params_for_xgboost method of the Model_Finder class")
        try:
            #initialization with different combination of parametrs
            self.param_grid_xgboost = {'learning_rate' : [0.5, 0.1, 0.01, 0.001], 'max_depth' : [3,5,10,20], 'n_estimators' : [10,50,100,200]}

            # creating an object of the grid search class
            self.grid = GridSearchCV(XGBClassifier(objective = 'binary:logistic'), self.param_grid_xgboost, verbose=3, cv = 5)
            #finding the best parameters
            self.grid.fit(train_x, train_y)

            #extracting the best parameters
            self.learning_rate = self.grid.best_params_['learning_rate']
            self.max_depth = self.grid.best_params_['max_depth']
            self.n_estimators = self.grid.best_params_['n_estimators']

            #Creating a new model with the best parameters
            self.xgb = XGBClassifier(learning_rate = self.learning_rate, max_depth = self.max_depth, n_estimators = self.n_estimators)
            #training the new model
            self.xgb.fit(train_x, train_y)
            self.logger_object.log(self.file_object, "XGBoost best params: ' +str(self.grid.best_params_) + '. Exited the get_best_params_for_xgboost method of the Model_Finder class")
            return self.xgb
        except Exception as e:
            self.logger_object.log(self.file_object, "Exception Occured in get_best_params_for_xgboost method of the Model_Finder class. Exception message: " +str(e))

            self.logger_object.log(self.file_object, "XGBoost Parameter tuning failed. Exited the get_best_params_for_xgboost method of the Model_Finder class")
            raise Exception()
        
    def get_best_model(self, train_x, train_y, test_x, test_y):
        """
                Method Name:- get_best_model
                Description:- Find out the model which has the best AUC score.
                Output:- The best model name and the model object
                On Failure:- Raise Exception

                Written By:- Sachin Bhumihar
                Versiom:- 1.0
                Revision:- None

        """    
        self.logger_object.log(self.file_object, "Entered the get_best_model method of the Model_Finder class")
        # Create best model for XGBoost
        try:
            self.xgboost = self.get_best_params_for_xgboost(train_x, train_y)
            self.prediction_xgboost = self.xgboost.predict(test_x) # Predictions using the XGboost Model

            if len(test_y.unique()) == 1: # if there is only one label in y, then ruc_auc_curve return error. we will use accuracy in that case
                self.xgboost_score = accuracy_score(test_y, self.prediction_xgboost)
                self.logger_object.log(self.file_object, "Accuracy for XGBoost:" +str(self.xgboost_score))# Log AUC
            else:
                self.xgboost_score = roc_auc_score(test_y, self.prediction_xgboost) # Auc for XGBoost
                self.logger_object.log(self.file_object, "AUC for XGBoost:" +str(self.xgboost_score)) #Log AUC

            #create best model for Random Forest
            self.random_forest = self.get_best_params_for_random_forest(train_x, train_y)
            self.prediction_random_forest = self.random_forest.predict(test_x)# Predictions using the Random Forest Algorithm

            if len(test_y.unique()) == 1: # if there is only one label in test_y then roc_auc score returns Error. we will use accuracy in that case 
                self.random_forest_score = accuracy_score(test_y, self.prediction_random_forest)
                self.logger_object.log(self.file_object, "Accuracy for RF:" + str(self.random_forest_score))
            else:
                self.random_forest_score = roc_auc_score(test_y, self.prediction_random_forest)
                self.logger_object.log(self.file_object, "AUC for RF:" +str(self.random_forest_score))

            #Comparing the two models
            if (self.random_forest_score < self.xgboost_score):
                return "XGBoost", self.xgboost
            else:
                return "RandomForest", self.random_forest

        except Exception as e:
            self.logger_object.log(self.file_object, "Exception Occured in get_best_model method of the Model_Finder class. Exception Message: " +str(e)) 
            self.logger_object.log(self.file_object, "Model Selection Failded. Exited the get_best_model method of the Model_Finder class)")
            raise Exception()
        
                            