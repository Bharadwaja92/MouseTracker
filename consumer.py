from confluent_kafka import Consumer
import json
from datetime import datetime
import MySQLdb
from MySQLdb import _mysql
import redis5


config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test_group',
    'auto.offset.reset': 'earliest',
}

sql_db = MySQLdb.connect(host='localhost', user='mouseuser', password='yourpassword', database='mouse_pos_db') 
cursor = sql_db.cursor()
sql_db.query('SELECT COUNT(*) FROM mouse_positions;')
resultset = sql_db.store_result()
num_rows = resultset.fetch_row()[0][0]
print('Num rows in db =', num_rows)

redis_conn = redis5.Redis(host='localhost', port=6379, db=0)

consumer = Consumer(config)
consumer.subscribe(['mouse_pos_topic'])
curr_pos = num_rows

print('Listening for messages...')

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            value = msg.value().decode('utf-8')
            data = json.loads(value)
            x = float(data.get('x', 0))
            y = float(data.get('y', 0))
            ts = data.get('time', datetime.now().isoformat())

            print(f"Received: x={x}, y={y}, time={ts}")

            cursor.execute(
                'INSERT INTO mouse_positions (x_coord, y_coord, timestamp) VALUES (%s, %s, %s)', (x, y, ts)
            )
            sql_db.commit()
            curr_pos += 1
            redis_conn.set(f'mouse_pos{curr_pos}', json.dumps({"x": x, "y": y, "timestamp": ts}))

        except Exception as e:
            print('Got error', e)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()



