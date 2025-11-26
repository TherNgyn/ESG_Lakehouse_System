FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    confluent-kafka \
    redis \
    python-dotenv

COPY scripts/ /app/scripts/

CMD ["python", "/app/scripts/kafka_redis_consumer.py"]