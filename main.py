import time
from kafka_client import create_topic, send_message
from data_generator import (
    generate_customer,
    generate_product,
    generate_transaction,
    generate_product_view,
    generate_user_interaction,
    generate_system_log
)
import threading
from dotenv import load_dotenv
import os

load_dotenv()
topic_name = os.getenv('KAFKA_TOPIC', 'stream-topic')
num_partitions = int(os.getenv('KAFKA_PARTITIONS', 2))
replication_factor = int(os.getenv('KAFKA_REPLICATION', 1))

# Stream customer data
def stream_customer_data(topic):
    print(f"Starting customer data stream to topic '{topic}'...")
    try:
        while True:
            customer = generate_customer()
            send_message(topic, customer)
            time.sleep(1)  # Adjust frequency as needed
    except KeyboardInterrupt:
        print("\nCustomer data streaming stopped.")

# Stream product data
def stream_product_data(topic):
    print(f"Starting product data stream to topic '{topic}'...")
    try:
        while True:
            product = generate_product()
            send_message(topic, product)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nProduct data streaming stopped.")

# Stream transaction data
def stream_transaction_data(topic):
    print(f"Starting transaction data stream to topic '{topic}'...")
    try:
        while True:
            transaction = generate_transaction()
            send_message(topic, transaction)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nTransaction data streaming stopped.")

# Stream product view data
def stream_product_view_data(topic):
    print(f"Starting product view data stream to topic '{topic}'...")
    try:
        while True:
            product_view = generate_product_view()
            send_message(topic, product_view)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nProduct view data streaming stopped.")

# Stream user interaction data
def stream_user_interaction_data(topic):
    print(f"Starting user interaction data stream to topic '{topic}'...")
    try:
        while True:
            user_interaction = generate_user_interaction()
            send_message(topic, user_interaction)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nUser interaction data streaming stopped.")

# Stream system log data
def stream_system_log_data(topic):
    print(f"Starting system log data stream to topic '{topic}'...")
    try:
        while True:
            system_log = generate_system_log()
            send_message(topic, system_log)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nSystem log data streaming stopped.")

if __name__ == "__main__":
    # Create the necessary topics
    create_topic('customer_data', num_partitions, replication_factor)
    create_topic('product_data', num_partitions, replication_factor)
    create_topic('transaction_data', num_partitions, replication_factor)
    create_topic('product_view_data', num_partitions, replication_factor)
    create_topic('user_interaction_data', num_partitions, replication_factor)
    create_topic('system_log_data', num_partitions, replication_factor)

    # Start streaming data in separate threads
    threads = [
        threading.Thread(target=stream_customer_data, args=('customer_data',)),
        threading.Thread(target=stream_product_data, args=('product_data',)),
        threading.Thread(target=stream_transaction_data, args=('transaction_data',)),
        threading.Thread(target=stream_product_view_data, args=('product_view_data',)),
        threading.Thread(target=stream_user_interaction_data, args=('user_interaction_data',)),
        threading.Thread(target=stream_system_log_data, args=('system_log_data',))
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
