FROM bitnami/spark:3.5.0

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy Streaming Script
COPY process_stream.py .

CMD ["spark-submit", "process_stream.py"]
