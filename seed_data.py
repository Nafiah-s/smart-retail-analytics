import pandas as pd
import numpy as np
from sqlalchemy import text
from db_utils import get_engine
import random
from datetime import datetime, timedelta

def seed_data():
    engine = get_engine()
    

    products = [
        ('Laptop', 'Electronics', 1200.00),
        ('Smartphone', 'Electronics', 800.00),
        ('Headphones', 'Electronics', 150.00),
        ('T-Shirt', 'Clothing', 25.00),
        ('Jeans', 'Clothing', 50.00),
        ('Sneakers', 'Clothing', 80.00),
        ('Coffee Maker', 'Home', 100.00),
        ('Blender', 'Home', 60.00),
        ('Desk Lamp', 'Home', 40.00),
        ('Backpack', 'Accessories', 45.00)
    ]
    
    products_df = pd.DataFrame(products, columns=['name', 'category', 'price'])
    

    customers = [
        ('John Doe', 'john@example.com', 'New York'),
        ('Jane Smith', 'jane@example.com', 'Los Angeles'),
        ('Alice Johnson', 'alice@example.com', 'Chicago'),
        ('Bob Brown', 'bob@example.com', 'Houston'),
        ('Charlie Davis', 'charlie@example.com', 'Phoenix')
    ]
    customers_df = pd.DataFrame(customers, columns=['full_name', 'email', 'city'])


    sales_data = []
    start_date = datetime.now() - timedelta(days=730)
    

    
    with engine.connect() as conn:

        
        products_df.to_sql('products', con=conn, if_exists='append', index=False)
        customers_df.to_sql('customers', con=conn, if_exists='append', index=False)
        

        existing_products = pd.read_sql("SELECT product_id, price FROM products", conn)
        existing_customers = pd.read_sql("SELECT customer_id FROM customers", conn)
        
        product_ids = existing_products['product_id'].tolist()
        product_prices = existing_products.set_index('product_id')['price'].to_dict()
        customer_ids = existing_customers['customer_id'].tolist()

        print(f"Seeding sales for {len(product_ids)} products and {len(customer_ids)} customers...")

        for _ in range(2000): # Generate 2000 sales records
            sale_date = start_date + timedelta(days=random.randint(0, 730))
            product_id = random.choice(product_ids)
            customer_id = random.choice(customer_ids)
            quantity = random.randint(1, 5)
            price = product_prices[product_id]
            total_amount = price * quantity
            
            sales_data.append({
                'product_id': product_id,
                'customer_id': customer_id,
                'quantity': quantity,
                'total_amount': total_amount,
                'sale_date': sale_date.date()
            })
            
        sales_df = pd.DataFrame(sales_data)
        sales_df.to_sql('sales', con=conn, if_exists='append', index=False)


        inventory_data = []
        for pid in product_ids:
            inventory_data.append({
                'product_id': pid,
                'stock_available': random.randint(0, 100), # Some might be 0 for alerts
                'last_restock': (datetime.now() - timedelta(days=random.randint(1, 30))).date()
            })
        inventory_df = pd.DataFrame(inventory_data)

        inventory_df.to_sql('inventory', con=conn, if_exists='replace', index=False)

    print("Data seeding completed successfully.")

if __name__ == "__main__":
    try:
        seed_data()
    except Exception as e:
        print(f"Error seeding data: {e}")
        print("Ensure MySQL is running and the database 'retail_db' exists.")
