from flask import Flask, request, jsonify
from pyspark.ml.regression import LinearRegressionModel

app = Flask(__name__)

# Load the saved Spark model
model_path = "path/to/your/model"
spark_session = SparkSession.builder.appName("ModelServingApp").getOrCreate()
model = LinearRegressionModel.load(spark_session, model_path)

@app.route('/predict', methods=['POST'])
def predict():
    data = request.json
    # Preprocess the input data (e.g., convert to Spark DataFrame)
    input_df = spark_session.createDataFrame([(data['feature1'], data['feature2'])], ["feature1", "feature2"])
    # Make predictions
    predictions = model.transform(input_df).collect()
    # Extract the prediction and return it
    prediction = predictions[0]["prediction"]
    return jsonify({'prediction': prediction})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)