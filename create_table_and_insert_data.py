import csv
import psycopg2
from datetime import datetime
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
    pizza_type_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    ingredients TEXT NOT NULL,
    UNIQUE (name, category, ingredients)
);

CREATE TABLE IF NOT EXISTS pizza (
    pizza_id VARCHAR(50) PRIMARY KEY,
    pizza_type_id VARCHAR(50) NOT NULL REFERENCES pizza_type(pizza_type_id),
    size VARCHAR(4) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    UNIQUE (pizza_type_id, size, price)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    time TIME NOT NULL,
    UNIQUE (date, time)
);

CREATE TABLE IF NOT EXISTS order_details (
    order_details_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(order_id),
    pizza_id VARCHAR(50) NOT NULL REFERENCES pizza(pizza_id),
    quantity INTEGER NOT NULL,
    UNIQUE (order_id, pizza_id, quantity)
);
"""

cur.execute(create_tables_query)
print("Created tables")

# Helper function to read CSV file and return data as list of tuples
def read_csv_file(file_name):
    with open(file_name, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        next(reader)  # Skip header row
        data = [tuple(row) for row in reader]
    return data

# Read data from CSV files
pizza_type_data = read_csv_file('pizza_types.csv_pizza_types.csv.csv')
pizza_data = read_csv_file('pizzas.csv_pizzas.csv.csv')
orders_data = read_csv_file('orders.csv_orders.csv.csv')
order_details_data = read_csv_file('order_details.csv_order_details.csv.csv')

print("Pizza Type Data:", pizza_type_data)
print("Pizza Data:", pizza_data)
print("Orders Data:", orders_data)
print("Order Details Data:", order_details_data)


# Insert data into pizza_type
insert_pizza_type_query = """
INSERT INTO pizza_type (pizza_type_id, name, category, ingredients)
VALUES (%s, %s, %s, %s)
ON CONFLICT (name, category, ingredients) DO NOTHING
"""
for row in pizza_type_data:
    try:
        # Print the row to verify its content
        print(f"Attempting to insert row: {row}")
        cur.execute(insert_pizza_type_query, row)
    except Exception as e:
        print(f"Error inserting row {row} into pizza_type: {e}")
print("Inserted pizza_type")
      
insert_pizza_query = """
INSERT INTO pizza (pizza_id, pizza_type_id, size, price)
VALUES (%s, %s, %s, %s)
ON CONFLICT (pizza_id) DO NOTHING
"""
for row in pizza_data:
    try:
        print(f"Attempting to insert into pizza: {row}")
        cur.execute(insert_pizza_query, row)
    except Exception as e:
        print(f"Error inserting row {row} into pizza: {e}")
print("Inserted pizza")

def convert_date_time(row):
    if len(row) != 3:
        print(f"Skipping row with unexpected number of columns: {row}")
        return None
    
    order_id_str, date_str, time_str = row
    try:
        order_id = int(order_id_str)
        date =date_str
        time = time_str
        return (order_id, date, time)
    except ValueError as e:
        print(f"Error parsing data: {e}")
        return None

orders_data = [convert_date_time(row) for row in orders_data if convert_date_time(row) is not None]
print("Converted Orders Data:", orders_data)  # Debugging line to see processed data

# Insert data into orders
insert_orders_query = """
INSERT INTO orders (order_id, date, time)
VALUES (%s, %s, %s)
ON CONFLICT (order_id) DO NOTHING
"""
for row in orders_data:
    try:
        print(f"Attempting to insert into orders: {row}")
        cur.execute(insert_orders_query, row)
    except Exception as e:
        print(f"Error inserting row {row} into orders: {e}")
print("Inserted orders")



insert_order_details_query = """
INSERT INTO order_details (order_details_id, order_id, pizza_id, quantity)
VALUES (%s, %s, %s, %s)
ON CONFLICT (order_details_id) DO NOTHING
"""
for row in order_details_data:
    try:
        # Convert order_details_id, order_id, quantity to integers
        row = (int(row[0]), int(row[1]), row[2], int(row[3]))
        print(f"Attempting to insert into order_details: {row}")
        cur.execute(insert_order_details_query, row)
    except Exception as e:
        print(f"Error inserting row {row} into order_details: {e}")
print("Inserted order_details")


# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
 
'''

drop_tables_query = """
DROP TABLE IF EXISTS order_details;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS pizza;
DROP TABLE IF EXISTS pizza_type;
"""

try:
    cur.execute(drop_tables_query)
    conn.commit()
    print("Tables dropped successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
    conn.rollback()
finally:
    # Close the cursor and connection
    cur.close()
    conn.close()

'''
