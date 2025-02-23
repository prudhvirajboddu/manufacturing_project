FROM bitnami/spark:latest
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["spark-submit", "process_stream.py"]