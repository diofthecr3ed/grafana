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
bucket = os.getenv("INFLUXDB_BUCKET", "lab2")

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
    last_people_count = None
    while True:
        people = int(random.uniform(0 , 5))
        temp = random.uniform(0.0, 40.0)
        co2 = random.uniform(0.0, 1500.0)
        aqi = random.uniform(0.0, 500.0)
      # Generate a random number of people in lab

        # Create a point for temperature, humidity, and pressure
        point = Point("lab data") \
            .tag("device", "device2") \
            .field("people", people) \
            .field("temperature", temp) \
            .field("aqi", aqi) \
            .field("co2", co2) \
            .time(time.time_ns(), WritePrecision.NS)

        write_api.write(bucket=bucket, org=org, record=point)


        time.sleep(5)

if __name__ == "__main__":
    logger.info("Starting data generation script...")
   

    generate_data()
