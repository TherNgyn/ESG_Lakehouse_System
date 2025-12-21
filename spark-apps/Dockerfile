FROM python:3.11-bullseye

ENV SPARK_VERSION="3.5.3"
ENV HADOOP_VERSION="3"
ENV SPARK_HOME="/opt/spark"
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

RUN groupadd -r spark && useradd -r -g spark -m -u 1000 spark

RUN chown -R spark:spark ${SPARK_HOME}
RUN mkdir -p /home/spark/.ivy2/cache && chown -R spark:spark /home/spark/.ivy2

RUN cd ${SPARK_HOME}/jars && \
    curl -L -o hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o spark-excel_2.12-3.3.1_0.18.5.jar \
        https://repo1.maven.org/maven2/com/crealytics/spark-excel_2.12/3.3.1_0.18.5/spark-excel_2.12-3.3.1_0.18.5.jar && \
    curl -L -o delta-spark_2.12-3.2.1.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar && \
    curl -L -o delta-storage-3.2.1.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar

RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    sentence-transformers \
    pyyaml \
    minio \
    openpyxl \
    pandas \
    boto3 \
    requests \
    delta-spark==3.2.1 \
    google-generativeai>=0.7.0 \
    pypdf

ENV PYTHONPATH="/opt/spark-apps:/opt/utils:${PYTHONPATH}"

USER spark
WORKDIR /opt/spark-apps

CMD ["/opt/spark/bin/spark-submit", \
      "--packages", "io.delta:delta-spark_2.12:3.2.1", \
      "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
      "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
      "--conf", "spark.databricks.delta.schema.autoMerge.enabled=true", \
      "/opt/spark-apps/kpi_schema.py"]