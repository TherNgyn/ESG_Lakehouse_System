FROM apache/spark:3.5.1-python3

USER root

# Tạo thư mục ivy2 cache với quyền phù hợp
RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark/.ivy2 && \
    chmod -R 755 /home/spark/.ivy2

RUN cd /opt/spark/jars && \
    curl -L -o hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    chown spark:spark hadoop-aws-3.3.4.jar aws-java-sdk-bundle-1.12.262.jar


USER spark

# Set working directory
WORKDIR /opt/spark/work-dir