import requests
from time import sleep
import random
import json
import sqlalchemy
import yaml
from sqlalchemy import text
from datetime import datetime
import time

random.seed(100)

API_URL = "https://9nf1524018.execute-api.us-east-1.amazonaws.com/v1/topics"

# Load database credentials from dbs_creds.yaml
def load_db_creds():
    with open("dbs_creds.yaml", "r") as file:
        creds = yaml.safe_load(file)
    return creds

class AWSDBConnector:
    def __init__(self):
        creds = load_db_creds()
        self.HOST = creds['RDS_HOST']
        self.USER = creds['RDS_USER']
        self.PASSWORD = creds['RDS_PASSWORD']
        self.DATABASE = creds['RDS_DATABASE']
        self.PORT = creds['RDS_PORT']

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

def convert_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert to ISO 8601 format string
    raise TypeError("Type not serializable")

def send_to_kinesis(topic, data, partition_key):
    topic_url = f"{API_URL}/{topic}"

    # Filter out unnecessary fields to reduce the payload size
    filtered_data = {k: v for k, v in data.items() if k in ["unique_id", "title", "category", "timestamp"]}  

    payload = {
        "records": [{"value": json.dumps(filtered_data, default=convert_datetime), "partitionKey": partition_key}]
    }

    headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}

    max_retries = 3  # Retry up to 3 times in case of failure
    for attempt in range(max_retries):
        response = requests.post(topic_url, headers=headers, data=json.dumps(payload))

        if response.status_code == 200:
            print(f"Successfully sent data to {topic}")
            return  # Exit if successful
        else:
            print(f"Failed to send data to {topic}, Status code: {response.status_code}. Attempt {attempt + 1} of {max_retries}")
            time.sleep(2)  # Wait before retrying

    print(f"Final attempt failed. Skipping data for {topic}.")

def run_infinite_post_data_loop():
    record_count = 0
    while record_count < 500:  

        sleep(random.uniform(1.5, 3.0))  # Slower requests to avoid overloading the API
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)

            send_to_kinesis("1215923e991d.pin", pin_result, "pinterest_data")
            send_to_kinesis("1215923e991d.geo", geo_result, "geolocation_data")
            send_to_kinesis("1215923e991d.user", user_result, "user_data")

        record_count += 3  # Increase record count by 3 for each loop (one from each table)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')

