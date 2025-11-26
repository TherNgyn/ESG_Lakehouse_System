#!/usr/bin/env python3
"""
Check Redis data for Grafana visualization
Displays current data stored in Redis for debugging
"""

import os
import json
import redis
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

def connect_redis():
    """Connect to Redis"""
    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )
    client.ping()
    return client

def print_section(title):
    """Print section header"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")

def check_timeseries(client, key, name):
    """Check time series data"""
    print(f"\n{name}:")
    length = client.llen(key)
    print(f"  Total records: {length}")
    
    if length > 0:
        # Get latest 5 entries
        latest = client.lrange(key, 0, 4)
        print(f"  Latest 5 readings:")
        for i, entry in enumerate(latest, 1):
            data = json.loads(entry)
            ts = datetime.fromtimestamp(data['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S')
            print(f"    {i}. {data['facility_name']} ({data['city']})")
            print(f"       PM2.5={data.get('pm25')}, PM10={data.get('pm10')}, AQI={data.get('aqi')}")
            print(f"       Time: {ts}")

def check_latest_readings(client, key):
    """Check latest readings per facility"""
    print(f"\nLatest Readings by Facility:")
    readings = client.hgetall(key)
    
    if not readings:
        print("  No data")
        return
    
    print(f"  Total facilities: {len(readings)}")
    for facility_id, data_json in readings.items():
        data = json.loads(data_json)
        ts = datetime.fromtimestamp(data['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S')
        
        pm25 = data.get('pm25', 0)
        status = "🟢" if pm25 < 50 else "🟡" if pm25 < 100 else "🔴"
        
        print(f"\n  {status} {data['facility_name']} ({data['city']})")
        print(f"     Type: {data['facility_type']}")
        print(f"     PM2.5: {data.get('pm25')} µg/m³")
        print(f"     PM10: {data.get('pm10')} µg/m³")
        print(f"     AQI: {data.get('aqi')}")
        print(f"     Last updated: {ts}")

def check_alerts(client, key):
    """Check recent alerts"""
    print(f"\nRecent Alerts:")
    alerts = client.lrange(key, 0, 9)
    
    if not alerts:
        print("  No alerts")
        return
    
    print(f"  Total alerts: {client.llen(key)}")
    print(f"  Latest 10:")
    
    for i, alert_json in enumerate(alerts, 1):
        alert = json.loads(alert_json)
        ts = datetime.fromtimestamp(alert['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S')
        
        severity_icon = "🚨" if alert['severity'] == 'critical' else "⚠️"
        
        print(f"\n  {i}. {severity_icon} {alert['message']}")
        print(f"     Facility: {alert['facility_name']} ({alert['city']})")
        print(f"     Value: {alert['value']} (Threshold: {alert['threshold']})")
        print(f"     Time: {ts}")

def check_facility_stats(client):
    """Check facility statistics"""
    print(f"\nFacility Statistics:")
    
    # Get all stats keys
    keys = client.keys("grafana:facility_stats:*")
    
    if not keys:
        print("  No statistics")
        return
    
    print(f"  Total categories: {len(keys)}")
    
    for key in sorted(keys):
        parts = key.split(':')
        facility_type = parts[2] if len(parts) > 2 else 'unknown'
        city = parts[3] if len(parts) > 3 else 'unknown'
        
        stats = client.hgetall(key)
        print(f"\n  {facility_type.upper()} - {city}")
        print(f"    Facilities: {len(stats)}")
        
        # Calculate averages
        pm25_values = []
        pm10_values = []
        aqi_values = []
        
        for facility_id, data_json in stats.items():
            data = json.loads(data_json)
            if data.get('pm25'):
                pm25_values.append(data['pm25'])
            if data.get('pm10'):
                pm10_values.append(data['pm10'])
            if data.get('aqi'):
                aqi_values.append(data['aqi'])
        
        if pm25_values:
            print(f"    Avg PM2.5: {sum(pm25_values)/len(pm25_values):.1f} µg/m³")
        if pm10_values:
            print(f"    Avg PM10: {sum(pm10_values)/len(pm10_values):.1f} µg/m³")
        if aqi_values:
            print(f"    Avg AQI: {sum(aqi_values)/len(aqi_values):.1f}")

def main():
    print("="*60)
    print(" Redis Data Inspector for Grafana")
    print("="*60)
    
    try:
        client = connect_redis()
        print(f"✓ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        
        # Check time series data
        print_section("Time Series Data")
        check_timeseries(client, "grafana:timeseries:pm25", "PM2.5 Time Series")
        check_timeseries(client, "grafana:timeseries:pm10", "PM10 Time Series")
        check_timeseries(client, "grafana:timeseries:aqi", "AQI Time Series")
        
        # Check latest readings
        print_section("Current Readings")
        check_latest_readings(client, "grafana:latest_readings")
        
        # Check alerts
        print_section("Alert History")
        check_alerts(client, "grafana:alerts")
        
        # Check statistics
        print_section("Aggregated Statistics")
        check_facility_stats(client)
        
        print("\n" + "="*60)
        print(" Inspection Complete")
        print("="*60 + "\n")
        
    except redis.ConnectionError as e:
        print(f"✗ Failed to connect to Redis: {e}")
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()