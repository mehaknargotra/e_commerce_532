# DATA FOR ANALYSIS 1: What is the distribution of active customers by their last login time, and how does it vary across different genders?
def generate_customer():
    customer = {
        "customer_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "location": fake.address(),
        "age": random.randint(18, 70),
        "gender": random.choice(["Male", "Female", "Other"]),
        "account_created": fake.past_date().isoformat(),
        "last_login": fake.date_time_this_month().isoformat(),
        "is_active": random.choice([True, False])  # Simulates active status
    }
    customers.append(customer["customer_id"])
    return customer


#DATA FOR ANALYSIS 2: What is the average price of products in each category, and how does it compare to overall market trends?
def generate_product():
    categories = ['Electronics', 'Books', 'Clothing', 'Home & Garden']
    # Simulated market average price and trend
    market_trends = {
        'Electronics': round(random.uniform(100, 300), 2),
        'Books': round(random.uniform(15, 50), 2),
        'Clothing': round(random.uniform(20, 100), 2),
        'Home & Garden': round(random.uniform(30, 150), 2)
    }
    category = random.choice(categories)
    product = {
        "product_id": fake.uuid4(),
        "name": fake.word().title(),
        "category": category,
        "price": round(random.uniform(10, 500), 2),
        "stock_quantity": random.randint(0, 100),
        "supplier": fake.company(),
        "rating": round(random.uniform(1, 5), 1),
        "market_avg_price": market_trends[category],  
    }
    products.append(product["product_id"])
    return product