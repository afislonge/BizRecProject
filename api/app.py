from flask import Flask, request, jsonify
from flask_cors import CORS
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, radians, lit, row_number, sqrt, sin, cos, asin,power
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
import pyspark
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

app = Flask(__name__)
CORS(app)

# Load the saved Spark model
model_path = 'https://drive.google.com/drive/folders/11u3h30KiyMwsFbr7eHsME9NuseNhI_xX?usp=sharing'
data_path = "..\Recommender_System_Newdata.csv"

try:
    spark_session = SparkSession.builder.appName("BizRecApi").getOrCreate()
    # spark_session.sparkContext.setLogLevel("ERROR")
    # model = LinearRegressionModel.load(spark_session, model_path)
except Exception as e:
    print(f"Error creating SparkSession: {e}")

@app.route("/")
def index():
    return "<p>BizRec System Api</p>"


#cache api response

@app.route('/settings', methods=['GET'])
def settings():
    spark_session = SparkSession.builder.appName("BizRecApi").getOrCreate()
    df_loc = spark_session.read.csv("data\city.csv", header=True, inferSchema=True) 
    loc_data = df_loc.rdd.flatMap(lambda x: x).collect()
    # loc_data =  ["Ontario", "Alberta", "Chicago"]
    cuz_data = ['American',
 'Burgers',
 'Cafe',
 'Chinese',
 'Creole',
 'Indian',
 'Italian',
 'Japanese',
 'Juice Bars',
 'Korean',
 'Mexican',
 'Seafood',
 'Southern',
 'Thai',
 'Vegan',
 'Vegetarian']
    
    return jsonify({"location" : loc_data, "cuizine" : cuz_data})

@app.route('/location', methods=['GET'])
def location():
    data =  ["Ontario", "Alberta", "Chicago"]
    return jsonify(data)

@app.route('/cuizine', methods=['GET'])
def cuizine():
    data = ["American", "Chinese", "Indian"]
    return jsonify(data)

@app.route('/predict', methods=['POST'])
def predict():
    req = request.json
    print(req.data)
    # Preprocess the input data (e.g., convert to Spark DataFrame)
    input_df = spark_session.createDataFrame([(req['user_location'], req['cuisine_type'], req['min_rating'], req['need_parking'], req['need_wifi'])])
    print(input_df)
    #read csv locatiom, filter csv by user location and return top 1\    
    df_loc = spark_session.read.csv("data\city_map.csv", header=True, inferSchema=True) 
    df_loc = df_loc.filter((col("city") == req['user_location']))
    df_loc = df_loc.limit(1)
    # get longitude and latirtude fron df_loc
    lat = df_loc.select("latitude").collect()[0][0]
    long = df_loc.select("longitude").collect()[0][0]
    print("")
    print(lat)
    print(long)
    spark_session = SparkSession.builder.appName("BizRecApi").getOrCreate()
    # Load the CSV file into a DataFrame
    # review_df = spark_session.read.csv(data_path, header=True, inferSchema=True)
    # review_df.show()

    # Make predictions
    model = LinearRegressionModel.load(spark_session, model_path)
    predictions = model.transform(input_df).collect()
    # Extract the prediction and return it
    prediction = predictions[0]["prediction"]
    return jsonify({'data': prediction})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)