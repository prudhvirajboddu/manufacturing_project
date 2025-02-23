from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pymongo import MongoClient
import logging
import time

# ✅ Setup Logging for Better Debugging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Retry Function for Spark Initialization
def initialize_spark():
    for i in range(5):  # Retry 5 times if Spark fails
        try:
            logging.info("🚀 Initializing Spark Session...")

            spark = SparkSession.builder \
                .appName("KafkaStreamingToMongo") \
                .config("spark.jars", "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/bitnami/spark/jars/kafka-clients-3.5.0.jar") \
                .getOrCreate()

            logging.info("✅ Spark Session Successfully Initialized!")
            return spark
        except Exception as e:
            logging.error(f"❌ Error Initializing Spark: {e}")
            time.sleep(5)  # Wait 5 seconds before retrying

    raise Exception("❌ Spark Initialization Failed After Multiple Attempts")

# ✅ Initialize Spark
spark = initialize_spark()

# ✅ Define Schema for Sensor Data
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("error_code", IntegerType(), True)
])

# ✅ Connect to Kafka Topic with Retry
def connect_to_kafka():
    for i in range(5):  # Retry up to 5 times
        try:
            logging.info("📡 Connecting to Kafka topic: manufacturing-data...")
            df = (
                spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")  # ✅ Use 'kafka' (Docker container name)
                .option("subscribe", "manufacturing-data")
                .option("startingOffsets", "latest")  # Start from the latest messages
                .load()
            )
            logging.info("✅ Connected to Kafka Successfully!")
            return df
        except Exception as e:
            logging.error(f"❌ Error Connecting to Kafka: {e}")
            time.sleep(5)

    raise Exception("❌ Kafka Connection Failed After Multiple Attempts")

df = connect_to_kafka()

# ✅ Parse JSON Data from Kafka
parsed_df = df.select(from_json(df.value.cast("string"), schema).alias("data")).select("data.*")

# ✅ Function to Write to MongoDB with Exception Handling
def write_to_mongo(batch_df, batch_id):
    try:
        logging.info(f"💾 Writing Batch {batch_id} to MongoDB...")

        records = batch_df.toPandas().to_dict(orient="records")

        client = MongoClient("mongodb://mongodb:27017/")
        db = client["manufacturing"]
        collection = db["sensor_data"]
        collection.insert_many(records)

        logging.info(f"✅ Successfully Written {len(records)} Records to MongoDB for Batch {batch_id}!")
    except Exception as e:
        logging.error(f"❌ Error Writing to MongoDB: {e}")

# ✅ Start Spark Streaming with Exception Handling
try:
    logging.info("🚀 Starting Spark Streaming...")
    query = parsed_df.writeStream \
        .foreachBatch(write_to_mongo) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
except Exception as e:
    logging.error(f"❌ Spark Streaming Failed: {e}")
