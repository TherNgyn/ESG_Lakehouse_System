FROM apache/spark:3.5.1-python3

USER root

RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark/.ivy2

# Download JAR files từ Maven (pre-download để tránh download lúc runtime)
RUN cd /opt/spark/jars && \
    curl -L -o hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    curl -L -o minio-8.5.2-all.jar https://repo1.maven.org/maven2/io/minio/minio/8.5.2/minio-8.5.2-all.jar

COPY ingest_data_bronze.py /opt/spark/app/ingest_data_bronze.py

RUN pip install boto3 minio pandas pyspark

USER spark