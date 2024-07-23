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

print("Question1")
count_orders_query = "SELECT COUNT(*) FROM orders"
try:
    cur.execute(count_orders_query)
    total_orders = cur.fetchone()[0]
    print(f"Total number of orders placed: {total_orders}")
except Exception as e:
    print(f"Error executing query: {e}")



print("Question2")
total_revenue_query = """
SELECT SUM(od.quantity * p.price) AS total_revenue
FROM order_details od
JOIN pizza p ON od.pizza_id = p.pizza_id
"""

try:
    # Execute the query
    cur.execute(total_revenue_query)
    # Fetch the result
    total_revenue = cur.fetchone()[0]
    print(f"Total revenue generated from pizza sales: ${total_revenue:.2f}")
except Exception as e:
    print(f"Error executing query: {e}")










# Close the cursor and connection
cur.close()
conn.close()
