from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark
from pyspark.sql import SparkSession
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Filter CSV Data") \
    .getOrCreate()

# Load the CSV file into a DataFrame
review_df = spark.read.csv("..\Recommender_System_Newdata.csv", header=True, inferSchema=True)
review_df.show(5)

