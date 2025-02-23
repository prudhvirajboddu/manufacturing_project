FROM bitnami/spark:3.5.0

WORKDIR /app

# ✅ Copy Kafka JARs into Spark's JARs Directory
COPY jars/*.jar /opt/bitnami/spark/jars/

# ✅ Install Python Dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# ✅ Copy Spark Streaming Script
COPY process_stream.py /app/

# ✅ Explicitly Pass Kafka JARs in Spark Submit Command
CMD ["spark-submit", "--jars", "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/bitnami/spark/jars/kafka-clients-3.5.0.jar", "/app/process_stream.py"]
