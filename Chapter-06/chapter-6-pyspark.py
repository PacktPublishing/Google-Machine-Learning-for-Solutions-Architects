from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Airbnb Data Cleaning and Preprocessing") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.21.1") \
    .getOrCreate()

source_dataset = "gs://GCS-BUCKET-NAME/data/AB_NYC_2019.csv"

# Define the schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("host_id", IntegerType(), True),
    StructField("host_name", StringType(), True),
    StructField("neighbourhood_group", StringType(), True),
    StructField("neighbourhood", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("room_type", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("minimum_nights", IntegerType(), True),
    StructField("number_of_reviews", IntegerType(), True),
    StructField("last_review", StringType(), True),
    StructField("reviews_per_month", StringType(), True),
    StructField("calculated_host_listings_count", IntegerType(), True),
    StructField("availability_365", IntegerType(), True),
])

# Load the dataset with the defined schema
data = spark.read.csv(source_dataset, header=True, schema=schema)

# Drop unnecessary columns
columns_to_drop = ["id", "name", "host_id", "host_name", "last_review", "reviews_per_month"]
data_cleaned = data.drop(*columns_to_drop)

# Filter rows with price between 10 and 800
data_cleaned = data_cleaned.filter((col("price") >= 10) & (col("price") <= 800))

# Cap minimum_nights at 30
data_cleaned = data_cleaned.withColumn("minimum_nights", when(col("minimum_nights") > 30, 30).otherwise(col("minimum_nights")))

# Feature selection
features = ["neighbourhood_group", "neighbourhood", "latitude", "longitude",
            "room_type", "minimum_nights", "number_of_reviews",
            "calculated_host_listings_count", "availability_365"]
target = "price"

# One-hot encoding and assembling features
numerical_columns = ["latitude", "longitude", "minimum_nights", "number_of_reviews",
                     "calculated_host_listings_count", "availability_365"]

# StringIndexer and OneHotEncoder
categorical_columns = ['neighbourhood_group', 'neighbourhood', 'room_type']

data_cleaned = data_cleaned.na.drop(subset=categorical_columns)

for col in numerical_columns:
    mean_value = data_cleaned.select(F.mean(F.col(col))).collect()[0][0]
    data_cleaned = data_cleaned.na.fill({col: mean_value})

indexers = [
    StringIndexer(inputCol=column, outputCol=column + "_index", handleInvalid="skip")
    for column in categorical_columns
]

encoders = [
    OneHotEncoder(inputCol=column + "_index", outputCol=column + "_onehot")
    for column in categorical_columns
]

assembler = VectorAssembler(
    inputCols=numerical_columns + [c + "_onehot" for c in categorical_columns],
    outputCol="features",
)
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler])
pipeline_model = pipeline.fit(data_cleaned)
X_encoded = pipeline_model.transform(data_cleaned)

# Selecting the target and features
X_encoded = X_encoded.select(F.col("scaled_features").alias("features"), F.col("price"))

# Convert the feature vector to a string so we can save it as a CSV file in GCS

def vector_to_string(vector):
    return str(vector)

vector_to_string_udf = udf(vector_to_string, StringType())

X_encoded = X_encoded.withColumn("features", vector_to_string_udf(F.col("features")))

# Specify GCS path
gcs_output_path = "gs://GCS-BUCKET-NAME/pyspark-airbnb"

# Write to GCS
X_encoded.write \
    .csv(gcs_output_path, mode="overwrite", header=True)
