import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from Phishing.File_Operations import File_Methods
from logging_app.logger import App_logger

class KMeansClustering:
    """
        This class is used to divide the data into clusters before training.
    """
    def __init__(self,file_object):
        self.logger = App_logger
        self.file_object = file_object

    def elbow_plot(self,data):
        """
             This function will plot elbow plot and tells how many clusters to be considered.
        """

        self.logger.log(self.file_object,'Entered the elbow_plot method of the KMeansClustering class')
        wcss=[]  ##initializing an empty list

        try:
            for i in range(1,11):
                kmeans = KMeans(n_clusters = i,init = 'k-means++',random_state=42) ##initializing kmeans object
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)

            plt.plot(range(1,11),wcss)  ##plotting graph between WCSS and the number of clusters
            plt.title('The Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            #plt.show()
            plt.savefig('Phishing/Preprocessing_Data/K-Means_Elbow.PNG') # saving the elbow plot locally

            # finding the value of optimum cluster
            self.kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            self.logger.log(self.file_object, 'The optimum number of clusters is: '+str(self.kn.knee)+' . Exited the elbow_plot method of the KMeansClustering class')
            return self.kn.knee

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in elbow_plot method of the KMeansClustering class. Exception message:  ' + str(e))
            self.logger.log(self.file_object,'Finding the number of clusters failed. Exited the elbow_plot method of the KMeansClustering class')
            raise Exception()

    def Create_Clusters(self,data,number_of_clusters):
        """
            Creating a new dataframe considering cluster information
        """

        self.logger.log(self.file_object, 'Entered the create_clusters method of the KMeansClustering class')
        self.data=data

        try:
            self.kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', random_state=42)
            #self.data = self.data[~self.data.isin([np.nan, np.inf, -np.inf]).any(1)]
            self.y_kmeans=self.kmeans.fit_predict(data) #  divide data into clusters

            self.file_op = file_methods.File_Operation(self.file_object,self.logger_object)
            self.save_model = self.file_op.save_model(self.kmeans, 'KMeans') # saving the KMeans model to directory
                                                                            # passing 'Model' as the functions need three parameters

            self.data['Cluster']=self.y_kmeans  # create a new column in dataset for storing the cluster information
            self.logger.log(self.file_object, 'succesfully created '+str(self.kn.knee)+ 'clusters. Exited the create_clusters method of the KMeansClustering class')
            return self.data

        except Exception as e:
            self.logger.log(self.file_object,'Exception occured in create_clusters method of the KMeansClustering class. Exception message:  ' + str(e))
            self.logger.log(self.file_object,'Fitting the data to clusters failed. Exited the create_clusters method of the KMeansClustering class')
            raise Exception()

            

