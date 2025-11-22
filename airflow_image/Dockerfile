from apache/airflow:latest
USER root
RUN apt-get update && apt-get install -y \
    poppler-utils \
    tesseract-ocr \
    tesseract-ocr-vie \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxrender1 \
    libxext6 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt