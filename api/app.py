from flask import Flask, request, jsonify
from flask_cors import CORS
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, radians, lit, row_number, sqrt, sin, cos, asin,power
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler

app = Flask(__name__)
CORS(app)

# Load the saved Spark model
model_path = 'https://drive.google.com/drive/folders/11u3h30KiyMwsFbr7eHsME9NuseNhI_xX?usp=sharing'
data_path = 'https://drive.google.com/drive/folders/1-HpqapXqc3LoGh4NzkKwX01yX-rsepEW?usp=sharing'

try:
    spark_session = SparkSession.builder.appName("BizRecApi").getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")
    model = LinearRegressionModel.load(spark_session, model_path)
except Exception as e:
    print(f"Error creating SparkSession: {e}")

@app.route("/")
def index():
    return "<p>BizRec System Api</p>"

@app.route('/settings', methods=['GET'])
def settings():
    loc_data =  ["Ontario", "Alberta", "Chicago"]
    cuz_data = ["American", "Chinese", "Indian"]
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
    # Make predictions
    predictions = model.transform(input_df).collect()
    # Extract the prediction and return it
    prediction = predictions[0]["prediction"]
    return jsonify({'data': prediction})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)