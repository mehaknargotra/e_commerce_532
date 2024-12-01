import json
import random
import time
from datetime import datetime
from faker import Faker
# from kafka import KafkaProducer
import os
from concurrent.futures import ThreadPoolExecutor


fake = Faker()

customers = []
products = []
# Generate Customer Data
def generate_customer():
    customer = {
        "customer_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "location": fake.address(),
        "age": random.randint(18, 70),
        "gender": random.choice(["Male", "Female", "Other"]),
        "account_created": fake.past_date().isoformat(),
        "last_login": fake.date_time_this_month().isoformat()
    }
    customers.append(customer["customer_id"])
    return customer
# Generate Product Data
def generate_product():
    categories = ['Electronics', 'Books', 'Clothing', 'Home & Garden']
    product = {
        "product_id": fake.uuid4(),
        "name": fake.word().title(),
        "category": random.choice(categories),
        "price": round(random.uniform(10, 500), 2),
        "stock_quantity": random.randint(0, 100),
        "supplier": fake.company(),
        "rating": round(random.uniform(1, 5), 1)
    }
    products.append(product["product_id"])
    return product
# Generate Transaction Data
def generate_transaction():
    customer_id = random.choice(customers)
    product_id = random.choice(products)
    return {
        "transaction_id": fake.uuid4(),
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": random.randint(1, 5),
        "date_time": fake.date_time_this_year().isoformat(),
        "status": random.choice(["completed", "pending", "canceled"]),
        "payment_method": random.choice(["credit card", "PayPal", "bank transfer"])
    }
# Generate Product View Data
def generate_product_view():
    return {
        "view_id": fake.uuid4(),
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "timestamp": fake.date_time_this_year().isoformat(),
        "view_duration": random.randint(10, 300)  # Duration in seconds
    }
# Generate System Log Data
def generate_system_log():
    log_levels = ["INFO", "WARNING", "ERROR"]
    return {
        "log_id": fake.uuid4(),
        "timestamp": fake.date_time_this_year().isoformat(),
        "level": random.choice(log_levels),
        "message": fake.sentence()
    }
# Generate User Interaction Data
def generate_user_interaction():
    interaction_types = ["wishlist_addition", "review", "rating"]
    return {
        "interaction_id": fake.uuid4(),
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "timestamp": fake.date_time_this_year().isoformat(),
        "interaction_type": random.choice(interaction_types),
        "details": fake.sentence() if interaction_types == "review" else None
    }
##add this code to send data to Kafka
# # Function to send data to Kafka
# def send_data():
#     # Occasionally add new customers or products
#     if random.random() < 0.5:
#         customer = generate_customer()
#         producer.send('ecommerce_customers', value=customer)
#     else:
#         product = generate_product()
#         producer.send('ecommerce_products', value=product)
#     # Higher chance to create transactions and interactions
#     if customers and products:
#         transaction = generate_transaction()
#         producer.send('ecommerce_transactions', value=transaction)
#         product_view = generate_product_view()
#         if product_view:
#             producer.send('ecommerce_product_views', value=product_view)
#         user_interaction = generate_user_interaction()
#         if user_interaction:
#             producer.send('ecommerce_user_interactions', value=user_interaction)
#     producer.send('ecommerce_system_logs', value=generate_system_log())
# # Parallel Data Generation
# with ThreadPoolExecutor(max_workers=5) as executor:
#     while True:
#         executor.submit(send_data)
#         time.sleep(random.uniform(0.01, 0.1))


#####CODE TO REMOVE (added by mehak)###########
def main():
    # Number of records to generate
    num_customers = 10
    num_products = 10
    num_transactions = 20
    num_product_views = 20
    num_system_logs = 10
    num_user_interactions = 15

    # Generate customer data
    print("Generating Customer Data...")
    customer_data = [generate_customer() for _ in range(num_customers)]
    print(json.dumps(customer_data, indent=4))

    # Generate product data
    print("\nGenerating Product Data...")
    product_data = [generate_product() for _ in range(num_products)]
    print(json.dumps(product_data, indent=4))

    # Generate transaction data
    print("\nGenerating Transaction Data...")
    transaction_data = [generate_transaction() for _ in range(num_transactions)]
    print(json.dumps(transaction_data, indent=4))

    # Generate product view data
    print("\nGenerating Product View Data...")
    product_view_data = [generate_product_view() for _ in range(num_product_views)]
    print(json.dumps(product_view_data, indent=4))

    # Generate system log data
    print("\nGenerating System Log Data...")
    system_log_data = [generate_system_log() for _ in range(num_system_logs)]
    print(json.dumps(system_log_data, indent=4))

    # Generate user interaction data
    print("\nGenerating User Interaction Data...")
    user_interaction_data = [generate_user_interaction() for _ in range(num_user_interactions)]
    print(json.dumps(user_interaction_data, indent=4))

if __name__ == "__main__":
    main()


