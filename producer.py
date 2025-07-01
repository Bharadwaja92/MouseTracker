from confluent_kafka import Producer
from random import random, randint
from datetime import datetime as dt
import time
import json

config = {
    'bootstrap.servers': 'localhost:9092'
}

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


producer = Producer(config)

# producer.produce('test_topic', 
#                  key='key_test', value='value_test', 
#                  callback=delivery_report)


## Send 5 Random coords and save in SQL and Redis
for i in range(5):
    mouse_pos = {'x': random()*randint(1, 10), 
                 'y': random()*randint(1, 10),
                 'time': str(dt.now())
                 }
    producer.produce('mouse_pos_topic', json.dumps(mouse_pos), callback=delivery_report)
    time.sleep(1)   # Sleep for 1 second

producer.flush()
