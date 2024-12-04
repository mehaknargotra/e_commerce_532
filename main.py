import time
from kafka_client import create_topic, send_message
from data_generator import generate_customer, generate_product, generate_product_view, generate_transaction, generate_user_interaction, generate_system_log
import threading
from dotenv import load_dotenv
import os

load_dotenv()
topic_name = os.getenv('KAFKA_TOPIC', 'stream-topic')
num_partitions = int(os.getenv('KAFKA_PARTITIONS', 2))
replication_factor = int(os.getenv('KAFKA_REPLICATION', 1))

def stream_customer_data(topic):

    print(f"Starting data stream to topic '{topic}'...")
    try:
        while True:
 
            customer = generate_customer()
#         producer.send('ecommerce_customers', value=customer)
            send_message(topic, customer)

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStreaming stopped.")

def stream_product_data(topic):

    print(f"Starting data stream to topic '{topic}'...")
    try:
        while True:
 
            product = generate_product()
#         producer.send('ecommerce_customers', value=customer)
            send_message(topic, product)

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStreaming stopped.")



if __name__ == "__main__":
    # Create the topic
    create_topic('customer_data', num_partitions, replication_factor)
    create_topic('product_data', num_partitions, replication_factor)
    create_topic('product_view', num_partitions, replication_factor)
    create_topic('transaction_data', num_partitions, replication_factor)
    create_topic('user_interaction', num_partitions, replication_factor)
    create_topic('system_log', num_partitions, replication_factor)
    # Start streaming data
    customer_thread = threading.Thread(target=stream_customer_data, args=('customer_data',))
    product_thread = threading.Thread(target=stream_product_data, args=('product_data',))

    # Start threads
    customer_thread.start()
    product_thread.start()

    # Wait for threads to finish (infinite loop)
    customer_thread.join()
    product_thread.join()