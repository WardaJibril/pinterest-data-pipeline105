import requests
from time import sleep
import random
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

random.seed(100)

API_URL = "https://9nf1524018.execute-api.us-east-1.amazonaws.com/v1/topics"

class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def convert_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert to ISO 8601 format string
    raise TypeError("Type not serializable")

def send_to_kafka(topics, data):
    
    topic_url = f"{API_URL}/{topics}"

    payload = {
        "records": [{"value": json.dumps(data, default=convert_datetime)}]
    }

    
    headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}

    
    response = requests.post(topic_url, headers=headers, data=json.dumps(payload))
    
    if response.status_code == 200:
        print(f"Successfully sent data to {topics}")
    else:
        print(f"Failed to send data to {topics}, Status code: {response.status_code}")


def run_infinite_post_data_loop():
    record_count = 0
    while record_count < 500:  # Send approximately 500 records

        sleep(random.randrange(0, 2))
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
            
            send_to_kafka("1215923e991d.pin", pin_result)  # Send to the 'pin' topic
            send_to_kafka("1215923e991d.geo", geo_result)  # Send to the 'geo' topic
            send_to_kafka("1215923e991d.user", user_result)  # Send to the 'user' topic

        sleep(random.uniform(0.5, 1.5))  # Sleep to simulate delay




if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


