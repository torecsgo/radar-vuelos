FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark_streaming.py .
COPY data/ ./data/

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

CMD ["python", "spark_streaming.py"]