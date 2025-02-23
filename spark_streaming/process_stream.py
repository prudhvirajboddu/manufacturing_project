from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pymongo import MongoClient
import logging

# ✅ Setup Logging for Debugging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamingToMongo") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

logging.info("✅ Spark Session Successfully Initialized!")

# ✅ Define Schema for Sensor Data
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("error_code", IntegerType(), True)
])

# ✅ Read Stream from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "manufacturing-data")
    .option("startingOffsets", "latest")
    .load()
)

# ✅ Parse JSON Data from Kafka
parsed_df = df.select(from_json(df.value.cast("string"), schema).alias("data")).select("data.*")

# ✅ Function to Write to MongoDB
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

# ✅ Start Streaming Query
query = parsed_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start()

query.awaitTermination()
