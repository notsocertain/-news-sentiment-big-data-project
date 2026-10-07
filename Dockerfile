# Use an Apache Spark image with Python and Scala 2.12, matching the connector artifacts.
FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    nano \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/news-sentimental
COPY . /opt/news-sentimental
RUN pip install --no-cache-dir -r requirements.txt
