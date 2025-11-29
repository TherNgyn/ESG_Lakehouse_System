FROM apache/spark:3.5.1-python3

USER root

RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark/.ivy2

RUN cd /opt/spark/jars && \
    curl -L -o hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

COPY ingest_data_bronze.py run.sh bronze_to_silver.py gold.py /opt/spark-apps/

RUN chmod +x /opt/spark-apps/run.sh && \
    pip install --no-cache-dir boto3 minio pandas openpyxl

USER spark