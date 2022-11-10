from wsgiref import simple_server
from flask import Flask, request
from flask import Response
import os
from flask_cors import CORS, cross_origin
from Phishing.Prediction_Validation_Insertion import Pred_Validation
from Phishing.TrainingModel import TrainModel
from Phishing.Training_Validation_Insertion import Train_Validation
import flask_monitoringdashboard as dashboard
from Phishing.PredictFromModel import Prediction



app = Flask(__name__)
dashboard.bind(app)
CORS(app)



@app.route("/train", methods=['POST'])
@cross_origin()
def TrainRouteClient():

    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']
            train_valObj = Train_Validation(path) #object initialization

            train_valObj.Train_Validation()#calling the training_validation function


            trainModelObj = TrainModel() #object initialization
            trainModelObj.TrainingModel() #training the model for the files in the table


    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")


@app.route("/predict", methods=['POST'])
@cross_origin()
def PredictRouteClient():
    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']

            pred_val = Pred_Validation(path) #object initialization

            pred_val.Prediction_Validation() #calling the prediction_validation function

            pred = Prediction(path) #object initialization

            # predicting for dataset present in database
            path = pred.PredictionFromModel()
            return Response("Prediction File created at %s!!!" % path)

    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)


#port = int(os.getenv("PORT"))
if __name__ == "__main__":
    host = '127.0.0.1'
    port = 5000
    httpd = simple_server.make_server(host, port, app)
    print("Serving on %s %d" % (host, port))
    httpd.serve_forever()

    

