#!/usr/bin/env python3
"""
Kafka to Redis Consumer - Direct streaming for Grafana
Reads PM sensor data from Kafka and writes to Redis for real-time Grafana visualization
"""

import os
import json
import time
import redis
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_SENSOR', 'pm-sensor-data')
KAFKA_GROUP_ID = 'grafana-redis-consumer'

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

# Redis keys theo format Grafana yêu cầu
REDIS_TIMESERIES_PM25 = "grafana:timeseries:pm25"
REDIS_TIMESERIES_PM10 = "grafana:timeseries:pm10"
REDIS_TIMESERIES_AQI = "grafana:timeseries:aqi"
REDIS_LATEST_READINGS = "grafana:latest_readings"
REDIS_FACILITY_STATS = "grafana:facility_stats"
REDIS_ALERTS = "grafana:alerts"

MAX_TIMESERIES_LENGTH = 1000  
MAX_ALERTS_LENGTH = 100

# ============================================
# Redis Client
# ============================================
def init_redis_client():
    max_retries = 10
    for attempt in range(max_retries):
        try:
            client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            client.ping()
            print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            return client
        except redis.ConnectionError as e:
            print(f"Attempt {attempt+1}/{max_retries}: Redis not ready ({e})")
            time.sleep(2)
    
    raise Exception("Failed to connect to Redis")

def init_kafka_consumer():
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 30000,
        'max.poll.interval.ms': 300000
    }
    
    consumer = Consumer(config)
    consumer.subscribe([KAFKA_TOPIC])
    print(f"Subscribed to Kafka topic: {KAFKA_TOPIC}")
    return consumer

def process_sensor_data(redis_client, data):
    try:
        facility_id = data.get('facility_id')
        facility_name = data.get('facility_name')
        facility_type = data.get('facility_type')
        city = data.get('city')
        
        pm25 = data.get('pm25')
        pm10 = data.get('pm10')
        aqi = data.get('aqi')
        
        timestamp = data.get('ts', int(time.time() * 1000))

        if not all([facility_id, pm25, pm10]):
            print(f"Skipping incomplete data for {facility_id}")
            return
       
        # Format dữ liệu time-series cho Grafana
        # Mỗi facility là một series riêng
        pm25_key = f"{REDIS_TIMESERIES_PM25}:{facility_id}"
        pm10_key = f"{REDIS_TIMESERIES_PM10}:{facility_id}"
        aqi_key = f"{REDIS_TIMESERIES_AQI}:{facility_id}"
        
        # Lưu dữ liệu time-series: timestamp -> value
        redis_client.zadd(pm25_key, {str(pm25): timestamp})
        redis_client.zremrangebyrank(pm25_key, 0, -MAX_TIMESERIES_LENGTH-1)
        
        redis_client.zadd(pm10_key, {str(pm10): timestamp})
        redis_client.zremrangebyrank(pm10_key, 0, -MAX_TIMESERIES_LENGTH-1)
        
        if aqi:
            redis_client.zadd(aqi_key, {str(aqi): timestamp})
            redis_client.zremrangebyrank(aqi_key, 0, -MAX_TIMESERIES_LENGTH-1)
        
        # Lưu metadata của facility
        metadata_key = f"{REDIS_TIMESERIES_PM25}:metadata:{facility_id}"
        redis_client.hset(metadata_key, mapping={
            'facility_name': facility_name,
            'facility_type': facility_type,
            'city': city
        })
        
        # Lưu latest readings cho table view
        redis_client.hset(
            REDIS_LATEST_READINGS,
            facility_id,
            json.dumps({
                'facility_name': facility_name,
                'facility_type': facility_type,
                'city': city,
                'pm25': pm25,
                'pm10': pm10,
                'aqi': aqi,
                'timestamp': timestamp,
                'last_updated': datetime.fromtimestamp(timestamp/1000).isoformat()
            })
        )
        
        # Statistics by facility type and city
        stats_key = f"{REDIS_FACILITY_STATS}:{facility_type}:{city}"
        redis_client.hset(stats_key, facility_id, json.dumps({
            'facility_name': facility_name,
            'pm25': pm25,
            'pm10': pm10,
            'aqi': aqi,
            'timestamp': timestamp
        }))

        # Alert processing
        alert = None
        if pm25 and pm25 >= 150:  
            alert = {
                'severity': 'critical',
                'facility_id': facility_id,
                'facility_name': facility_name,
                'city': city,
                'metric': 'PM2.5',
                'value': pm25,
                'threshold': 150,
                'message': f'PM2.5 đạt mức nguy hại: {pm25} µg/m³',
                'timestamp': datetime.fromtimestamp(timestamp/1000).isoformat()
            }
        elif pm25 and pm25 >= 100: 
            alert = {
                'severity': 'warning',
                'facility_id': facility_id,
                'facility_name': facility_name,
                'city': city,
                'metric': 'PM2.5',
                'value': pm25,
                'threshold': 100,
                'message': f'PM2.5 ở mức không lành mạnh: {pm25} µg/m³',
                'timestamp': datetime.fromtimestamp(timestamp/1000).isoformat()
            }
        
        if alert:
            redis_client.lpush(REDIS_ALERTS, json.dumps(alert))
            redis_client.ltrim(REDIS_ALERTS, 0, MAX_ALERTS_LENGTH - 1)
            print(f"🚨 Alert: {alert['message']} at {facility_name}")
        
        # Log status
        status_icon = "🟢" if pm25 < 50 else "🟡" if pm25 < 100 else "🔴"
        print(f"{status_icon} {facility_name} ({city}): PM2.5={pm25}, PM10={pm10}, AQI={aqi}")
        
    except Exception as e:
        print(f"✗ Error processing data: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("="*60)
    print("Kafka → Redis Consumer for Grafana")
    print("="*60)
    
    # Initialize clients
    redis_client = init_redis_client()
    kafka_consumer = init_kafka_consumer()
    
    print("\n✓ Consumer ready. Processing messages...")
    print("(Press Ctrl+C to stop)\n")
    
    message_count = 0
    
    try:
        while True:
            msg = kafka_consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"✗ Kafka error: {msg.error()}")
                    continue
            
            # Parse message
            try:
                value = msg.value().decode('utf-8')
                data = json.loads(value)
                
                # Process and write to Redis
                process_sensor_data(redis_client, data)
                
                message_count += 1
                if message_count % 10 == 0:
                    print(f"\n📊 Processed {message_count} messages")
                
            except json.JSONDecodeError as e:
                print(f"✗ JSON decode error: {e}")
            except Exception as e:
                print(f"✗ Error processing message: {e}")
    
    except KeyboardInterrupt:
        print("\n\n✓ Consumer stopped by user")
    
    finally:
        kafka_consumer.close()
        redis_client.close()
        print("✓ Cleanup complete")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)