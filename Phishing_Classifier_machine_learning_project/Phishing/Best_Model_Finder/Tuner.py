from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score,accuracy_score
from xgboost.core import Objective
from logging_app.logger import App_logger

class Model_Finder:
    """
        This function is used to find the model with best accuracy and AUC score.
    """

    def __init__(self,file_object):
        self.file_object = file_object
        self.logger = App_logger
        self.sv_classifier = SVC()
        self.xgb = XGBClassifier(Objective='binary:logistic',n_jobs = -1)

    def Get_Best_Params_For_Svm(self,x_train,y_train):
        """
            This function is used to get the parameters for the SVM algorithm with the best accuracy.Use hyperparameter tuning.
        """

        self.logger.log(self.file_object, 'Entered the get_best_params_for_svm method of the Model_Finder class')

        try:
            ##initializing with different combinations of parameters.
            self.param_grid = {"kernel": ['rbf', 'sigmoid'],"C": [0.1, 0.5, 1.0],"random_state": [0, 100, 200, 300]}

            ##creating an object of grid search class
            self.grid = GridSearchCV(estimator=self.sv_classifier, param_grid=self.param_grid, cv=5,  verbose=3)

            ##finding best parameters
            self.grid.fit(x_train,y_train)

            ##extracting best parameters
            self.kernel = self.grid.best_params_['kernel']
            self.C = self.grid.best_params_['C']
            self.random_state = self.grid.best_params_['random_state']

            ##creating new model with best parameters
            self.sv_classifier = SVC(kernel=self.kernel,C=self.C,random_state=self.random_state)

            ##training new model
            self.sv_classifier.fit(x_train,y_train)
            self.logger.log(self.file_object,'SVM best params: '+str(self.grid.best_params_)+'. Exited the get_best_params_for_svm method of the Model_Finder class')
            return self.sv_classifier

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in get_best_params_for_svm method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger.log(self.file_object,'SVM training  failed. Exited the get_best_params_for_svm method of the Model_Finder class')
            raise Exception()

    def Get_Best_Params_For_Xgboost(self,x_train,y_train):

        """
            This function is used to get  the parameters for XGBoost Algorithm which give the best accuracy.Use Hyper Parameter Tuning.
        """

        self.logger.log(self.file_object,'Entered the get_best_params_for_xgboost method of the Model_Finder class')

        try:
            # initializing with different combination of parameters
            self.param_grid_xgboost = {"n_estimators": [100, 130], "criterion": ['gini', 'entropy'],"max_depth": range(8, 10, 1)}

            # Creating an object of the Grid Search class
            self.grid= GridSearchCV(XGBClassifier(objective='binary:logistic'),self.param_grid_xgboost, verbose=3,cv=5)

            # finding the best parameters
            self.grid.fit(x_train,y_train)

            # extracting the best parameters
            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']
            self.n_estimators = self.grid.best_params_['n_estimators']

            # creating a new model with the best parameters
            self.xgb = XGBClassifier(criterion=self.criterion, max_depth=self.max_depth,n_estimators= self.n_estimators, n_jobs=-1 )

            # training the mew model
            self.xgb.fit(x_train,y_train)
            self.logger.log(self.file_object,'XGBoost best params: ' + str(self.grid.best_params_) + '. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            return self.xgb

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in get_best_params_for_xgboost method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'XGBoost Parameter tuning  failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise Exception()


    def Get_Best_Model(self,x_train,y_train,x_test,y_test):
        """
            This function is used to find the Model which has the best AUC score.
        """

        self.logger.log(self.file_object,'Entered the get_best_model method of the Model_Finder class')

        # create best model for XGBoost
        try:
            self.xgboost= self.get_best_params_for_xgboost(x_train,y_train)
            self.prediction_xgboost = self.xgboost.predict(x_test) # Predictions using the XGBoost Model

            if len(y_test.unique()) == 1: #if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.xgboost_score = accuracy_score(y_test, self.prediction_xgboost)
                self.logger.log(self.file_object, 'Accuracy for XGBoost:' + str(self.xgboost_score))  # Log AUC

            else:
                self.xgboost_score = roc_auc_score(y_test, self.prediction_xgboost) # AUC for XGBoost
                self.logger.log(self.file_object, 'AUC for XGBoost:' + str(self.xgboost_score)) # Log AUC

            # create best model for Random Forest
            self.svm=self.get_best_params_for_svm(x_train,y_train)
            self.prediction_svm=self.svm.predict(x_test) # prediction using the SVM Algorithm

            if len(y_test.unique()) == 1:#if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.svm_score = accuracy_score(y_test,self.prediction_svm)
                self.logger.log(self.file_object, 'Accuracy for SVM:' + str(self.svm_score))

            else:
                self.svm_score = roc_auc_score(y_test, self.prediction_svm) # AUC for Random Forest
                self.logger.log(self.file_object, 'AUC for SVM:' + str(self.svm_score))

            #comparing the two models
            if(self.svm_score <  self.xgboost_score):
                return 'XGBoost',self.xgboost
            else:
                return 'SVM',self.sv_classifier

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in get_best_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger.log(self.file_object,'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise Exception()


