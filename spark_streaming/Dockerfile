FROM bitnami/spark:3.5.0

WORKDIR /app

# Copy Pre-Downloaded Kafka JARs into Spark's JARs Directory
COPY jars/*.jar /opt/bitnami/spark/jars/

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy Spark Streaming Script
COPY process_stream.py .

CMD ["spark-submit", "process_stream.py"]
