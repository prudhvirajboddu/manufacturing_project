from pyspark.sql import SparkSession
from pymongo import MongoClient
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .getOrCreate()

# Define Schema for JSON Data
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("error_code", IntegerType(), True)
])

#  Corrected Syntax - Read Stream from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "manufacturing-data")
    .option("startingOffsets", "latest")  # Start from latest data
    .load()  
)

#  Process Kafka Messages
df = (
    df.selectExpr("CAST(value AS STRING)")  # Convert Kafka message to String
    .select(from_json(col("value"), schema).alias("data"))  # Parse JSON
    .select("data.*")  # Extract fields
)


# Function to write batch data to MongoDB
def write_to_mongo(batch_df, batch_id):
    records = batch_df.toPandas().to_dict(orient="records") 
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["manufacturing"]
    collection = db["sensor_data"]
    collection.insert_many(records)

# Start Streaming
query = df.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start()

query.awaitTermination()
