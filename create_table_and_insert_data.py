import psycopg2
from psycopg2 import sql
import pandas as pd
import os
# Database connection parameters
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgresql',
    'host': 'localhost',
    'port': '5432'
}

# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Create the tables only if they do not exist
create_tables_query = """
CREATE TABLE IF NOT EXISTS pizza_type (
    pizza_type_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    ingredients TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pizza (
    pizza_id SERIAL PRIMARY KEY,
    pizza_type_id INTEGER NOT NULL REFERENCES pizza_type(pizza_type_id),
    size VARCHAR(20) NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    time TIME NOT NULL
);

CREATE TABLE IF NOT EXISTS order_details (
    order_details_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(order_id),
    pizza_id INTEGER NOT NULL REFERENCES pizza(pizza_id),
    quantity INTEGER NOT NULL
);
"""

cur.execute(create_tables_query)
print("Successful creation")


import os

def rename(current_file_name,new_file_name):
    # Check if the current file exists
    if os.path.exists(current_file_name):
        try:
            # Rename the file
            os.rename(current_file_name, new_file_name)
            print(f"File renamed from {current_file_name} to {new_file_name}")
        except PermissionError:
            print(f"Error: You do not have permission to rename '{current_file_name}'.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
    else:
        print(f"Error: The file '{current_file_name}' does not exist.")


rename("pizza_types.csv_pizza_types.csv","pizza_types.csv")
rename('pizzas.csv_pizzas.csv','pizzas.csv')


pizza_type_df = pd.read_csv('pizza_types.csv_pizza_types.csv')
pizza_df = pd.read_csv('pizzas.csv_pizzas.csv')
orders_df = pd.read_csv('orders.csv_orders.csv')
order_details_df = pd.read_csv('order_details.csv_order_details.csv')

insert_pizza_type_query = """
INSERT INTO pizza_type (name, category, ingredients)
VALUES (%s, %s, %s)
ON CONFLICT (name, category, ingredients) DO NOTHING
"""
cur.executemany(insert_pizza_type_query, pizza_type_df.values)

# Insert data into pizza
insert_pizza_query = """
INSERT INTO pizza (pizza_type_id, size, price)
VALUES (%s, %s, %s)
ON CONFLICT (pizza_type_id, size, price) DO NOTHING
"""
cur.executemany(insert_pizza_query, pizza_df.values)

# Insert data into orders
insert_orders_query = """
INSERT INTO orders (date, time)
VALUES (%s, %s)
ON CONFLICT (date, time) DO NOTHING
"""
cur.executemany(insert_orders_query, orders_df.values)

# Insert data into order_details
insert_order_details_query = """
INSERT INTO order_details (order_id, pizza_id, quantity)
VALUES (%s, %s, %s)
ON CONFLICT (order_id, pizza_id, quantity) DO NOTHING
"""
cur.executemany(insert_order_details_query, order_details_df.values)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
print("Successful insertion")
