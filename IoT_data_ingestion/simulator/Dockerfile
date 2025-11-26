FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY ./simulator/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts
COPY ./simulator/iot_simulator.py /app/scripts/

# Create data directory
RUN mkdir -p /app/data

CMD ["python", "/app/scripts/iot_simulator.py"]