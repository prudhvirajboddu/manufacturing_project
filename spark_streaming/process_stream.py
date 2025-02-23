from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pymongo import MongoClient

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .config("spark.jars", "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/bitnami/spark/jars/kafka-clients-3.5.0.jar") \
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

# Read Stream from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "manufacturing-data")
    .option("startingOffsets", "latest")
    .load()
)

# Process Data
df = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# Write to MongoDB
def write_to_mongo(batch_df, batch_id):
    records = batch_df.toPandas().to_dict(orient="records")
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["manufacturing"]
    collection = db["sensor_data"]
    collection.insert_many(records)

df.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
