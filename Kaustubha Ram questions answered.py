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

def execute_query(query, conn):
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchall()
    return result

# Q1: The total number of orders placed
query1 = "SELECT COUNT(DISTINCT order_id) AS total_orders FROM orders;"
total_orders = execute_query(query1, conn)
print("Question1: ")
print("Total number of orders placed:", total_orders[0][0])



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

print("Question3")
highest_price_pizza = """
select * from
(select * from pizza_type pt
inner join pizza p on
pt.pizza_type_id=p.pizza_type_id
"""

try:
    # Execute the query
    cur.execute(highest_price_pizza)
    # Fetch the result
    total_revenue = cur.fetchone()[0]
    print(f"Total revenue generated from pizza sales: ${total_revenue:.2f}")
except Exception as e:
    print(f"Error executing query: {e}")










# Close the cursor and connection
cur.close()
conn.close()
