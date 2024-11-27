from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Yelp Data Processing").getOrCreate()

# Define GCS bucket path
#gs://yelp-dataproc/scripts/yelp_data_processing.py
gcs_bucket = "gs://yelp-dataproc/data/"

# Load CSV files into Spark DataFrames
business_df = spark.read.csv(gcs_bucket + "yelp_business.csv", header=True, inferSchema=True)
review_df = spark.read.csv(gcs_bucket + "yelp_review.csv", header=True, inferSchema=True)
user_df = spark.read.csv(gcs_bucket + "yelp_user.csv", header=True, inferSchema=True)
checkin_df = spark.read.csv(gcs_bucket + "yelp_checkin.csv", header=True, inferSchema=True)
tip_df = spark.read.csv(gcs_bucket + "yelp_tip.csv", header=True, inferSchema=True)

# Data Processing Examples
# 1. Join business data with reviews
business_reviews = business_df.join(review_df, business_df["business_id"] == review_df["business_id"]) \
    .select(business_df["name"], business_df["city"], review_df["stars"].alias("review_stars"))

# 2. Average review stars per business
avg_review_stars = business_reviews.groupBy("name", "city").avg("review_stars").orderBy("avg(review_stars)", ascending=False)

# 3. Count tips for each business
tips_count = tip_df.groupBy("business_id").count().alias("tip_count")

# 4. Combine user details with reviews
user_reviews = user_df.join(review_df, user_df["user_id"] == review_df["user_id"]) \
    .select(user_df["name"], review_df["business_id"], review_df["stars"])

# Save Processed Data to GCS
output_path = "gs://yelp-dataproc/yelp_output/"
avg_review_stars.write.csv(output_path + "avg_review_stars", header=True)
tips_count.write.csv(output_path + "tips_count", header=True)
user_reviews.write.csv(output_path + "user_reviews", header=True)

print("Data processing completed. Outputs written to:", output_path)
