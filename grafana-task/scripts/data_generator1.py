from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time
import random
import urllib3
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# InfluxDB connection details from environment variables
url = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
token = os.getenv("INFLUXDB_TOKEN", "XdDgvjQXTymljILun5Ellh3iAmCUv2KStpBK4qSpnS13Sw0LIAw-3UBPcFpB9x7HOfxoXvY1ZEEVtJrDHpd-bw==")
org = os.getenv("INFLUXDB_ORG", "my-org")
bucket = os.getenv("INFLUXDB_BUCKET", "iot_data")

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

def wait_for_influxdb():
    while True:
        try:
            health = client.health()
            if health.status == 'pass':
                break
        except urllib3.exceptions.NewConnectionError:
            pass
        logger.info("Waiting for InfluxDB to be ready...")
        time.sleep(5)

def generate_data():
    wait_for_influxdb()
    
    devices = [
        {"name": "device1"},
        {"name": "device2"},
        {"name": "device3"}
    ]
    
    last_people_counts = {device["name"]: None for device in devices}
    
    while True:
        for device in devices:
            temperature = random.uniform(20.0, 30.0)
            humidity = random.uniform(30.0, 70.0)
            pressure = random.uniform(1.0, 5.0)
              # Generate a random number of people in lab
            
            # Create a point for temperature, humidity, and pressure
            point = Point("iot_metrics") \
                .tag("device", device["name"]) \
                .field("temperature", temperature) \
                .field("humidity", humidity) \
                .field("pressure", pressure) \
                .time(time.time_ns(), WritePrecision.NS)
            
            # Add people_count field only if it has changed
    
            # Write the point to InfluxDB
            write_api.write(bucket=bucket, org=org, record=point)
            logger.info(f"Data written: device={device['name']}, temperature={temperature}, humidity={humidity}, pressure={pressure}, people_count={people_count if people_count != last_people_counts[device['name']] else 'unchanged'}")
        
        time.sleep(5)

if __name__ == "__main__":
    logger.info("Starting data generation script...")
    generate_data()
