import time
from kafka_client import create_topic, send_message
from data_generator import generate_customer

from dotenv import load_dotenv
import os

load_dotenv()
topic_name = os.getenv('KAFKA_TOPIC', 'stream-topic')
num_partitions = int(os.getenv('KAFKA_PARTITIONS', 2))
replication_factor = int(os.getenv('KAFKA_REPLICATION', 1))

def stream_data():

    print(f"Starting data stream to topic '{topic_name}'...")
    try:
        while True:
 
            customer = generate_customer()
#         producer.send('ecommerce_customers', value=customer)
            send_message(topic_name, customer)

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStreaming stopped.")

if __name__ == "__main__":
    # Create the topic
    create_topic(topic_name, num_partitions, replication_factor)

    # Start streaming data
    stream_data()
