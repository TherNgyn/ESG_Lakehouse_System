import os
import json
import time
import ssl
import requests
from datetime import datetime
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from minio import Minio
from minio.error import S3Error
from io import BytesIO

load_dotenv()

# ============================================
# Configuration
# ============================================
MQTT_BROKER = os.environ["MQTT_BROKER"]
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USERNAME = os.environ["MQTT_USERNAME"]
MQTT_PASSWORD = os.environ["MQTT_PASSWORD"]
MQTT_TOPIC = os.environ.get("MQTT_TOPIC_SENSOR", "pm-sensor-data")
DELAY_SEC = float(os.environ.get("SENDER_DELAY_SEC", "300"))  # 5 minutes
WAQI_TOKEN = os.environ.get("WAQI_TOKEN")

# MinIO Configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "lakehouse")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"

# ============================================
# Facility Metadata
# ============================================
FACILITIES_METADATA = {
    "warehouse_hanoi": {
        "facility_id": "WH_HN_001",
        "facility_type": "warehouse",
        "facility_name": "Kho vận Hà Nội",
        "city": "Hanoi",
        "latitude": 21.0285,
        "longitude": 105.8542,
        "api_station": "@1594"
    },
    "warehouse_hcm": {
        "facility_id": "WH_HCM_001", 
        "facility_type": "warehouse",
        "facility_name": "Kho vận TP.HCM",
        "city": "Ho Chi Minh",
        "latitude": 10.7756,
        "longitude": 106.7019,
        "api_station": "A565432"
    },
    "factory_danang": {
        "facility_id": "FAC_DN_001",
        "facility_type": "factory",
        "facility_name": "Nhà máy Đà Nẵng",
        "city": "Da Nang",
        "latitude": 16.0544,
        "longitude": 108.2022,
        "api_station": "@1584"
    },
    "factory_gialai": {
        "facility_id": "FAC_GL_001",
        "facility_type": "factory",
        "facility_name": "Nhà máy Gia Lai",
        "city": "Gia Lai",
        "latitude": 13.9833,
        "longitude": 108.0000,
        "api_station": "@13417"
    },
    "factory_dongnai": {
        "facility_id": "FAC_DN_002",
        "facility_type": "factory",
        "facility_name": "Nhà máy Đồng Nai",
        "city": "Dong Nai",
        "latitude": 10.9500,
        "longitude": 106.8167,
        "api_station": "@13687"
    },
    "farm_tuyenquang": {
        "facility_id": "FARM_TQ_001",
        "facility_type": "farm",
        "facility_name": "Trang trại Tuyên Quang",
        "city": "Tuyen Quang",
        "latitude": 21.8267,
        "longitude": 105.2280,
        "api_station": "@13660"
    },
    "farm_thanhhoa": {
        "facility_id": "FARM_TH_001",
        "facility_type": "farm",
        "facility_name": "Trang trại Thanh Hóa",
        "city": "Thanh Hoa",
        "latitude": 19.8067,
        "longitude": 105.7851,
        "api_station": "@13660"
    },
    "farm_nghean": {
        "facility_id": "FARM_NA_001",
        "facility_type": "farm",
        "facility_name": "Trang trại Nghệ An",
        "city": "Nghe An",
        "latitude": 18.6790,
        "longitude": 105.6819,
        "api_station": "@13660"
    },
    "farm_lamdong": {
        "facility_id": "FARM_LD_001",
        "facility_type": "farm",
        "facility_name": "Trang trại Lâm Đồng",
        "city": "Lam Dong",
        "latitude": 11.9465,
        "longitude": 108.4419,
        "api_station": "@13660"
    },
    "farm_tayninh": {
        "facility_id": "FARM_TN_001",
        "facility_type": "farm",
        "facility_name": "Trang trại Tây Ninh",
        "city": "Tay Ninh",
        "latitude": 11.3351,
        "longitude": 106.1098,
        "api_station": "@13660"
    }
}

# ============================================
# MinIO Client Setup
# ============================================
def init_minio_client():
    """Initialize MinIO client and ensure bucket exists"""
    try:
        client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )

        # Check if bucket exists
        if not client.bucket_exists(bucket_name=MINIO_BUCKET):
            client.make_bucket(bucket_name=MINIO_BUCKET)
            print(f"✓ Created MinIO bucket: {MINIO_BUCKET}")
        else:
            print(f"✓ MinIO bucket exists: {MINIO_BUCKET}")

        return client

    except S3Error as e:
        print(f"✗ MinIO connection error: {e}")
        return None
    except Exception as e:
        print(f"✗ MinIO initialization error: {e}")
        return None


# ============================================
# Save to Bronze Layer (MinIO)
# ============================================
def save_to_bronze_minio(minio_client, facility_key, data):
    if not minio_client:
        print("✗ MinIO client not available")
        return False
    
    try:
        date_str = datetime.now().strftime("%Y-%m-%d")
        timestamp = int(time.time() * 1000)

        facility_type = data["facility_type"]
        city = data["city"]

        object_name = (
            f"bronze/Vinamilk/pm_sensors/"
            f"facility_type={facility_type}/"
            f"city={city}/"
            f"date={date_str}/"
            f"{facility_key}_{timestamp}.json"
        )

        json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
        json_stream = BytesIO(json_bytes)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            data=json_stream,
            length=len(json_bytes),
            content_type='application/json'
        )

        print(f"✓ Saved to Bronze MinIO: {object_name}")
        return True

    except Exception as e:
        print(f"✗ Error saving to Bronze: {e}")
        return False

# ============================================
# Fetch AQI Data from API
# ============================================
def fetch_aqi_data(station_id):
    """Fetch PM2.5 and PM10 data from WAQI API"""
    try:
        url = f"https://api.waqi.info/feed/{station_id}/?token={WAQI_TOKEN}"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if data.get("status") != "ok":
            print(f"✗ API error for {station_id}: {data}")
            return None
            
        aqi_data = data.get("data", {})
        iaqi = aqi_data.get("iaqi", {})
        
        # Extract PM2.5 and PM10
        pm25 = iaqi.get("pm25", {}).get("v")
        pm10 = iaqi.get("pm10", {}).get("v")
        
        return {
            "pm25": pm25,
            "pm10": pm10,
            "aqi": aqi_data.get("aqi"),
            "timestamp": aqi_data.get("time", {}).get("iso"),
            "city": aqi_data.get("city", {}).get("name")
        }
    except Exception as e:
        print(f"✗ Error fetching data for {station_id}: {e}")
        return None

# ============================================
# MQTT Connection
# ============================================
connected = False

def on_connect(client, userdata, flags, rc):
    global connected
    if rc == 0:
        print(f"✓ MQTT connected to {MQTT_BROKER}:{MQTT_PORT}")
        connected = True
    else:
        print(f"✗ MQTT connection failed: {rc}")
        connected = False

def on_disconnect(client, userdata, rc):
    global connected
    print(f"MQTT disconnected: {rc}")
    connected = False

# ============================================
# Main Execution
# ============================================
if __name__ == "__main__":
    # Initialize MinIO
    print("Initializing MinIO client...")
    minio_client = init_minio_client()
    
    if not minio_client:
        print("Failed to initialize MinIO. Exiting.")
        exit(1)
    
    # Setup MQTT
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    ssl_context = ssl.create_default_context()
    client.tls_set_context(ssl_context)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    
    print(f"Connecting to MQTT: {MQTT_BROKER}:{MQTT_PORT}...")
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()
    
    # Wait for MQTT connection
    timeout = 10
    start_time = time.time()
    while not connected and (time.time() - start_time) < timeout:
        print("Waiting for MQTT connection...")
        time.sleep(1)
    
    if not connected:
        print("Failed to connect to MQTT")
        exit(1)
    
    print(f"\n{'='*60}")
    print("ESG AIR QUALITY MONITORING - IoT Simulator Started")
    print(f"Monitoring {len(FACILITIES_METADATA)} facilities")
    print(f"Update interval: {DELAY_SEC} seconds")
    print(f"Storage: MinIO Bronze Layer (Delta Lake)")
    print(f"{'='*60}\n")
    
    # Main loop
    iteration = 0
    while True:
        iteration += 1
        print(f"\n[Iteration {iteration}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        for facility_key, metadata in FACILITIES_METADATA.items():
            if not connected:
                print("MQTT disconnected, stopping...")
                break
            
            # Fetch data from API
            aqi_data = fetch_aqi_data(metadata["api_station"])
            
            if aqi_data is None:
                print(f"  ⊘ {metadata['facility_name']}: No data")
                continue
            
            # Create payload
            payload = {
                # Sensor metadata
                "sensor_id": f"{metadata['facility_id']}_PM_SENSOR_001",
                "facility_id": metadata["facility_id"],
                "facility_name": metadata["facility_name"],
                "facility_type": metadata["facility_type"],
                "city": metadata["city"],
                "latitude": metadata["latitude"],
                "longitude": metadata["longitude"],
                
                # Measurements
                "pm25": aqi_data["pm25"],
                "pm10": aqi_data["pm10"],
                "aqi": aqi_data["aqi"],
                
                # Timestamps
                "measurement_time": aqi_data["timestamp"],
                "ingestion_timestamp": datetime.now().isoformat(),
                "ts": int(time.time() * 1000),
                
                # Metadata
                "data_source": "waqi_api",
                "company": "Vinamilk",
                "esg_category": "Environmental",
                "layer": "bronze"
            }
            
            # Save to Bronze Layer in MinIO
            save_to_bronze_minio(minio_client, facility_key, payload)
            
            # Publish to MQTT (for real-time processing)
            try:
                msg = json.dumps(payload, ensure_ascii=False)
                result = client.publish(MQTT_TOPIC, msg, qos=1)
                
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    pm25_status = "🟢" if payload["pm25"] and payload["pm25"] < 50 else "🟡" if payload["pm25"] and payload["pm25"] < 100 else "🔴"
                    print(f"  {pm25_status} {metadata['facility_name']}: PM2.5={payload['pm25']}, PM10={payload['pm10']}, AQI={payload['aqi']}")
                else:
                    print(f"  ✗ MQTT publish failed: {result.rc}")
            except Exception as e:
                print(f"  ✗ Error publishing: {e}")
        
        print(f"\nWaiting {DELAY_SEC}s until next update...")
        time.sleep(DELAY_SEC)
    
    # Cleanup
    client.loop_stop()
    client.disconnect()
    print("Simulator stopped.")