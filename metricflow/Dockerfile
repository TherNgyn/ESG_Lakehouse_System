FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /usr/app

RUN apt-get update && \
    apt-get install -y git curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir \
    numpy==1.23.5 \
    pandas==1.5.3 \
    dbt-core==1.7.18 \
    dbt-trino==1.7.1 \
    dbt-metricflow==0.6.0 \
    gunicorn==21.2.0 \
    flask==3.0.0 \
    flask-cors==4.0.0 \
    requests==2.31.0
COPY api_server.py .
COPY . /usr/app
ENV PYTHONPATH=/usr/app
ENV DBT_PROJECT_DIR=/usr/dbt-project
ENV DBT_PROFILES_DIR=/usr/dbt-profiles
EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "api_server:app"]
