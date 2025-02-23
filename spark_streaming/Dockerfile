FROM apache/spark:3.5.0

# ✅ Install Python, Pip, and Required Packages
USER root
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install --upgrade pip && \
    pip3 install pymongo kafka-python

WORKDIR /app

# ✅ Copy Kafka JARs into Spark's JARs Directory
COPY jars/*.jar /opt/spark/jars/

# ✅ Copy Spark Streaming Script
COPY process_stream.py /app/

# ✅ Explicitly Pass Kafka JARs in Spark Submit Command
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "/app/process_stream.py"]