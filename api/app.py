from flask import Flask, request, jsonify
from flask_cors import CORS
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
import pyspark
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pyspark.sql.functions import col, expr, radians, lit, row_number, sqrt, sin, cos, asin, power, count, when, collect_list
from pyspark.ml import PipelineModel
import pandas as pd



app = Flask(__name__)
CORS(app)

# Load the saved Spark model
model_path ="restaurant_recommender_Final_model"
data_path = "Recommender_System_Newdata.csv"
review_model_path = "topic_senti_model"
try:
    spark_session = SparkSession.builder.appName("BizRecApi").getOrCreate()
    # spark_session.sparkContext.setLogLevel("ERROR")
    # model = LinearRegressionModel.load(spark_session, model_path)
except Exception as e:
    print(f"Error creating SparkSession: {e}")

class RestaurantRecommenderPredictor:
    def __init__(self, spark_session):
        """
        Initialize the Restaurant Recommender Predictor
        """
        self.spark = spark_session
        self.df = None
        self.kmeans_model = None
        self.cuisine_indexer = None
        self.vector_assembler = None
        self.scaler = None

    def load_data(self, data_path):
        """
        Load and preprocess restaurant data
        """
        # Read the CSV file
        self.df = self.spark.read.csv(data_path, header=True, inferSchema=True)

        # Data cleaning and preprocessing
        self.df = self.df.na.drop()  # Remove rows with null values

        # Convert boolean columns
        self.df = (self.df.withColumn("has_parking",
                    expr("CASE WHEN parking = 'Yes' THEN true ELSE false END"))
                   .withColumn("has_wifi",
                    expr("CASE WHEN WiFi = 'Yes' THEN true ELSE false END")))

        # Encode categorical variables
        self.cuisine_indexer = StringIndexer(
            inputCol="cuisine_type",
            outputCol="cuisine_type_encoded"
        )
        self.df = self.cuisine_indexer.fit(self.df).transform(self.df)

        return self.df

    def load_saved_model(self, model_path):
        """
        Load the saved PySpark Pipeline Model
        """
        try:
            self.kmeans_model = PipelineModel.load(model_path)

            # Extract specific stages from the pipeline
            stages = self.kmeans_model.stages

            # Find and store vector assembler and scaler
            for stage in stages:
                if isinstance(stage, VectorAssembler):
                    self.vector_assembler = stage
                elif isinstance(stage, StandardScaler):
                    self.scaler = stage

            return self.kmeans_model
        except Exception as e:
            print(f"Error loading model: {e}")
            return None

    def haversine_distance(self, lat1, lon1, lat2, lon2):
        """
        Calculate distance between two geographical points
        """
        lat1_rad = radians(lit(lat1))
        lon1_rad = radians(lit(lon1))
        lat2_rad = radians(lat2)
        lon2_rad = radians(lon2)

        return (
            6371 * 2 * asin(
                sqrt(
                    sin((lat2_rad - lat1_rad) / 2) ** 2 +
                    cos(lat1_rad) * cos(lat2_rad) *
                    sin((lon2_rad - lon1_rad) / 2) ** 2
                )
            )
        )

    def recommend_restaurants(
        self,
        user_location,
        min_rating,
        need_parking,
        need_wifi,
        cuisine_type,
        max_distance=60
    ):
        """
        Recommend restaurants based on user preferences
        """
        user_lat, user_lon = user_location

        # Apply initial filters
        filtered_df = self.df.filter(
            (col("rating") >= min_rating) &
            (col("cuisine_type") == cuisine_type)
        )

        # Apply parking filter
        if need_parking:
            filtered_df = filtered_df.filter(col("has_parking") == True)

        # Apply WiFi filter
        if need_wifi:
            filtered_df = filtered_df.filter(col("has_wifi") == True)

        # Add distance column
        with_distance_df = filtered_df.withColumn(
            "distance",
            self.haversine_distance(user_lat, user_lon, col("latitude"), col("longitude"))
        )

        # Filter by distance
        nearby_restaurants = with_distance_df.filter(
            col("distance") <= max_distance
        )

        # Rank restaurants
        window_spec = Window.partitionBy("bus_name").orderBy(col("rating").desc())

        recommended_restaurants = (
            nearby_restaurants
            .withColumn("rank", row_number().over(window_spec))
            .filter(col("rank") == 1)
            .orderBy(col("rating").desc())
            .select("business_id", "bus_name", "address", "rating", "distance", "cuisine_type")
            .limit(5)
        )

        return recommended_restaurants

    def analyze_sentiments_and_topics(self, recommendation_df, data_path, model_path):
        """
        Analyze sentiments and topics for recommended restaurants
        """
        # Load the saved model
        loaded_model = PipelineModel.load(model_path)

        # Load review data
        review_df = self.spark.read.csv(data_path, header=True, inferSchema=True).select("business_id", "rev_text")

        # Filter reviews for recommended restaurants
        business_ids = recommendation_df.select("business_id").distinct().rdd.flatMap(lambda x: x).collect()
        filtered_reviews = review_df.filter(col("business_id").isin(business_ids))

        # Transform reviews using the sentiment model
        predictions = loaded_model.transform(filtered_reviews)
        predictions = predictions.withColumnRenamed("prediction", "sentimentPrediction")

        # Calculate sentiment percentages
        sentiment_count = predictions.groupBy("business_id", "sentimentPrediction").agg(count("sentimentPrediction").alias("sentiment_count"))
        total_reviews = predictions.groupBy("business_id").agg(count("rev_text").alias("total_reviews"))

        sentiment_percentage = sentiment_count.join(total_reviews, on="business_id")
        sentiment_percentage = sentiment_percentage.withColumn(
            "sentiment_percentage",
            (col("sentiment_count") / col("total_reviews") * 100)
        )

        # Add topic analysis
        predictions = predictions.withColumn(
            "topic",
            when(col("rev_text").contains("food"), "food_quality")
            .when(col("rev_text").contains("service"), "customer_service")
            .when(col("rev_text").contains("clean"), "cleanliness")
            .otherwise("other")
        )

        topic_count = predictions.groupBy("business_id", "topic").agg(count("topic").alias("topic_count"))
        topic_percentage = topic_count.join(total_reviews, on="business_id")
        topic_percentage = topic_percentage.withColumn(
            "topic_percentage",
            (col("topic_count") / col("total_reviews") * 100)
        )

        return sentiment_percentage, topic_percentage

@app.route("/")
def index():
    return "<p>BizRec System Api</p>"

#cache api response

@app.route('/settings', methods=['GET'])
def settings():
    # spark_session = SparkSession.builder.appName("BizRecApi").getOrCreate()    
    # df_loc = spark_session.read.csv("data\city.csv", header=True, inferSchema=True) 
    # loc_data = df_loc.rdd.flatMap(lambda x: x).collect()
    # loc_data =  ["Ontario", "Alberta", "Chicago"]

    df_loc = pd.read_csv("data/city.csv", header=0)
    loc_data = df_loc.values.tolist()

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

def haversine_distance( lat1, lon1, lat2, lon2):
        """
        Calculate distance between two geographical points
        """
        lat1_rad = radians(lit(lat1))
        lon1_rad = radians(lit(lon1))
        lat2_rad = radians(lat2)
        lon2_rad = radians(lon2)

        return (
            6371 * 2 * asin(
                sqrt(
                    sin((lat2_rad - lat1_rad) / 2) ** 2 +
                    cos(lat1_rad) * cos(lat2_rad) *
                    sin((lon2_rad - lon1_rad) / 2) ** 2
                )
            )
        )

@app.route('/recommend', methods=['POST'])
def recommend():
    data = request.get_json()
    print(data)
    location = data['user_location']
    rating = data['min_rating']
    cuisine = data['cuisine_type']
    parking = (data['need_parking']).lower()
    wifi = (data['need_wifi']).lower()
    
    # # Preprocess the input data (e.g., convert to Spark DataFrame)      
    df_loc = pd.read_csv("data/city_map.csv", header=0) 
    filtered_df = df_loc[df_loc['city'] == location]
    lat = filtered_df.iloc[0]['latitude']
    long = filtered_df.iloc[0]['longitude']

    spark = SparkSession.builder \
        .appName("Restaurant Recommender and Sentiment Analyzer") \
        .getOrCreate()
    
    try:
    # Initialize predictor
        predictor = RestaurantRecommenderPredictor(spark)

        # Load dataset
        predictor.load_data(data_path)

        # Load saved model
        loaded_model = predictor.load_saved_model(model_path)

        if loaded_model:
            # Interactive recommendation
            recommendations = predictor.recommend_restaurants(
                user_location=(lat, long),
                min_rating=rating,
                need_parking=parking,
                need_wifi=wifi,
                cuisine_type=cuisine
            )

            # Analyze sentiments and topics
            sentiment_percentage, topic_percentage = predictor.analyze_sentiments_and_topics(
                recommendations, data_path, review_model_path
            )

            # print("Recommended:")
            # recommendations.show()

            # print("Sentiment Percentages:")
            # sentiment_percentage.show()

            # print("Topic Percentages:")
            # topic_percentage.show()

            Sentiment_df = sentiment_percentage.groupBy("business_id", "total_reviews").agg(
    collect_list("sentimentPrediction").alias("sentimentPrediction_list"),
    collect_list("sentiment_count").alias("sentiment_count_list"),
    collect_list("sentiment_percentage").alias("sentiment_percentage_list")
)
            # print("Sentiment:")
            # Sentiment_df.show()

            topic_df = topic_percentage.groupBy("business_id").agg(
    collect_list("topic").alias("topic_list"),
    collect_list("topic_count").alias("topic_count_list"),
     collect_list("topic_percentage").alias("topic_percentage_list")
)
            # print("Topic:")
            # topic_df.show()

            # Joining DataFrames
            final_df = (
                recommendations
                .join(Sentiment_df, on="business_id", how="inner")
                .join(topic_df, on="business_id", how="inner")
            )

            # Showing the result
            # final_df.show(truncate=False)
            final = final_df.toPandas()

            # recommendation = recommendations.toPandas()
            # sentiment = sentiment_percentage.toPandas()
            # topic = topic_percentage.toPandas()
            # pandas_df = pd.merge(recommendation, sentiment, on='business_id', how='inner')
            # pandas_df = pd.merge(pandas_df, topic, on='business_id', how='inner')
            # pandas_df = pandas_df.drop_duplicates(subset=['business_id'])
            # pandas_df = pandas_df.drop(columns=['business_id', 'sentimentPrediction', 'topic_count', 'total_reviews'])
            # pandas_df = pandas_df.rename(columns={'topic_percentage': 'topic'})
            # pandas_df = pandas_df.rename(columns={'sentiment_percentage': 'sentiment'})

            dict_data = final.to_dict(orient='records')            

        return jsonify({'message': 'Data received successfully','data': dict_data})

    except Exception as e:
        return jsonify({'message': f"An error occurred: {e}",'data': None})
    finally:
        spark.stop()
    
    #load the model
    # spark_session = SparkSession.builder.appName("BizRecApi").getOrCreate()

    # kmeans_model = PipelineModel.load(model_path)
    #         # Extract specific stages from the pipeline
    # stages = kmeans_model.stages
    #         # Find and store vector assembler and scaler
    # for stage in stages:
    #     if isinstance(stage, VectorAssembler):
    #         vector_assembler = stage
    #     elif isinstance(stage, StandardScaler):
    #         scaler = stage

    # df = spark_session.read.csv(data_path, header=True, inferSchema=True)
    # df = df.na.drop()
    # df = (df.withColumn("has_parking",
    #                 expr("CASE WHEN parking = 'Yes' THEN true ELSE false END"))
    #                .withColumn("has_wifi",
    #                 expr("CASE WHEN WiFi = 'Yes' THEN true ELSE false END")))

    #     # Encode categorical variables
    # cuisine_indexer = StringIndexer(
    #         inputCol="cuisine_type",
    #         outputCol="cuisine_type_encoded"
    #     )
    # df = cuisine_indexer.fit(df).transform(df)

    # filtered_df = df.filter(
    #         (col("rating") >= rating) &
    #         (col("cuisine_type") == cuisine)
    #     )

    #     # Apply parking filter
    # if parking:
    #     filtered_df = filtered_df.filter(col("has_parking") == True)

    #     # Apply WiFi filter
    # if wifi:
    #     filtered_df = filtered_df.filter(col("has_wifi") == True)

    #     # Add distance column
    # with_distance_df = filtered_df.withColumn(
    #         "distance",
    #         haversine_distance(lat, long, col("latitude"), col("longitude"))
    #     )

    #     # Filter by distance
    # max_distance=60
    # nearby_restaurants = with_distance_df.filter(
    #         col("distance") <= max_distance
    #     )

    #     # Rank restaurants
    # window_spec = Window.partitionBy("bus_name").orderBy(col("rating").desc())

    # recommended_restaurants = (
    #         nearby_restaurants
    #         .withColumn("rank", row_number().over(window_spec))
    #         .filter(col("rank") == 1)
    #         .orderBy(col("rating").desc())
    #         .select("business_id", "bus_name", "address", "rating", "distance", "cuisine_type")
    #         .limit(5)
    #     )

    # pandas_df = recommended_restaurants.toPandas()
    # dict_data = pandas_df.to_dict(orient='records')

    # return jsonify({'message': 'Data received successfully','data': dict_data})

    # return jsonify({'data': prediction})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)