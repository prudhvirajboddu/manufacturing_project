from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pymongo import MongoClient

# MongoDB Connection
mongo_client = MongoClient("mongodb://mongodb:27017/")
db = mongo_client["manufacturing"]
collection = db["sensor_data"]

# Spark Session
spark = SparkSession.builder.appName("KafkaStreaming").getOrCreate()

# Define Schema
schema = StructType([
    ("timestamp", StringType()),
    ("machine_id", StringType()),
    ("temperature", DoubleType()),
    ("vibration", DoubleType()),
    ("pressure", DoubleType()),
    ("error_code", IntegerType())
])

# Read from Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "manufacturing-data")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# Function to write to MongoDB
def write_to_mongo(batch_df, batch_id):
    records = batch_df.toPandas().to_dict(orient="records")
    collection.insert_many(records)

df.writeStream.foreachBatch(write_to_mongo).start().awaitTermination()