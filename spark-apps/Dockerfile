FROM apache/spark:3.5.1-python3

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create Ivy cache directory
RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark/.ivy2

#COPY ./jars/spark-excel_2.12-3.3.3_0.20.3.jar /opt/spark-jars/

# Download required JARs
RUN cd /opt/spark/jars && \
    curl -L -o hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o spark-excel_2.12-3.3.1_0.18.5.jar \
        https://repo1.maven.org/maven2/com/crealytics/spark-excel_2.12/3.3.1_0.18.5/spark-excel_2.12-3.3.1_0.18.5.jar

# Install Python packages
RUN pip install --no-cache-dir \
    pyyaml \
    minio \
    openpyxl \
    pandas \
    boto3

# Set Python path to include /opt/utils
ENV PYTHONPATH="/opt/spark-apps:/opt/utils:${PYTHONPATH}"

WORKDIR /opt/spark-apps