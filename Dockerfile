FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# 1. Install Google Cloud Connector for Hadoop/Spark
# This jar file allows Spark to talk to "gs://" buckets directly
RUN curl -o /usr/local/spark/jars/gcs-connector-hadoop3-latest.jar \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

# 2. Copy dependency file
COPY requirements.txt /tmp/

# 3. Install Python libraries
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 4. Switch back to the default user to avoid permission issues
USER ${NB_UID}