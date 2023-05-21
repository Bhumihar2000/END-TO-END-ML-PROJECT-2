from wsgiref import simple_server
from flask import Flask, request, render_template
from flask import Response
import os
from flask_cors import CORS, cross_origin
from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
import flask_monitoringdashboard as dashboard
from predictFromModel import prediction
import json
 

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')


@app.route("/predict", methods=["POST"])
@cross_origin()
def predictRouteClient():
    try:
        if request.json['filepath']:
            path = request.json['filepath']

            pred_val = pred_validation(path) # Object Initialization

            pred_val.prediction_validation() # calling the prediction_validation function

            pred = prediction(path) # Object Initialization

            # predicting for dataset present in database

            path, json_predictions = pred.predictionFromModel()
            return Response("Prediction File created at !!!" +str(path) + 'and few of the predictions are ' +str(json.loads(json_predictions)))

        elif request.form is not None:
            path = request.form['filepath'] 

            pred_val = pred_validation(path) # Object initialization

            pred_val.prediction_validation() # calling the prediction_validation function

            pred = prediction(path) # object Initialization

            # predicting for dataset present in database
            path, json_predictions = pred.predictionFromModel()
            return Response("Prediction File Created at !!!" +str(path) + 'and few of the predictions are ' +str(json.loads(json_predictions)))  
        
        else:
            print("Nothing Matched")

    except ValueError:
        return Response("Error Occured! %s" %ValueError)
    except KeyError:
        return Response("Error Occured! %s" %KeyError)
    except Exception as e:
        return Response("Error Occured! %s" %e)
            




@app.route("/train", methods=["POST"])
@cross_origin()
def trainRouteClient():
    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']

            train_valObj = train_validation(path) # Object initialization

            train_valObj.train_validation() # Calling the training validation function


            trainModelObj = trainModel() #Object initialization
            trainModelObj.trainingModel() # training the model for the files in the table


    except ValueError:
        return Response("Error Occurred! %s" % ValueError)

    except KeyError:
        return Response("Error Occurred! %s" % ValueError)

    except Exception as e:
        return Response("Error Occurred! %s" % e)
    return Response("Training Successfull!!")


port = int(os.getenv("PORT", 5000))
if __name__=="__main__":
    host = "0.0.0.0"
    #port = 5000
    httpd = simple_server.make_server(host, port, app)
    # print("Serving on %s %d" % (host, port))
    httpd.serve_forever()