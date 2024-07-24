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
print("")

# Q2:  The total revenue generated from pizza sales
query2 = """
SELECT SUM(od.quantity * p.price) AS total_revenue
FROM order_details od
JOIN pizza p ON od.pizza_id = p.pizza_id
"""
total_revenue=execute_query(query2,conn)
print("Question2: ")
print("Total revenue generated: ",total_revenue[0][0])
print("")

# Q3:   The highest priced pizza.
query3= """
select name,price from pizza_type pt inner join pizza p on
pt.pizza_type_id=p.pizza_type_id where price=(select MAX(Price) from pizza_type pt
inner join pizza p on
pt.pizza_type_id=p.pizza_type_id)
"""
highest_price_pizza=execute_query(query3,conn)
print("Question3: ")
print(f"Highest Price Pizza:{highest_price_pizza[0][0]} and it costs {highest_price_pizza[0][1]}")
print("")

# Q4:   The most common pizza size ordered.
query4="""
select name,sum(od.quantity) from pizza_type pt
inner join pizza p on pt.pizza_type_id=p.pizza_type_id
inner join order_details od on p.pizza_id=od.pizza_id
group by name,price order by sum(od.quantity) desc
limit 1;
"""
most_common_pizza=execute_query(query4,conn)
print("Question4: ")
print(f"Mot Common Pizza:{most_common_pizza[0][0]} and it was ordered {most_common_pizza[0][1]} times")
print("")

# Q5:   The top 5 most ordered pizza types along their quantities.
query5="""
select name,sum(od.quantity) from pizza_type pt
inner join pizza p on pt.pizza_type_id=p.pizza_type_id
inner join order_details od on p.pizza_id=od.pizza_id
group by name,price order by sum(od.quantity) desc
limit 5;
"""
top5_most_common_pizza=execute_query(query5,conn)
print("Question5: ")
print("5 Most Common Pizza: :")
for x in top5_most_common_pizza:
    print(f"{x[0]} was ordered {x[1]} times")
print("")

# Q6:   The quantity of each pizza categories ordered.
query6="""
select pt.pizza_type_id,sum(od.quantity) from pizza_type pt
inner join pizza p on pt.pizza_type_id=p.pizza_type_id
inner join order_details od on p.pizza_id=od.pizza_id
group by pt.pizza_type_id ;
"""
top5_most_common_pizza=execute_query(query6,conn)
print("Question6: ")
for x in top5_most_common_pizza:
    print(f"{x[0]} was ordered {x[1]} times")
print("")

# Q7:   The distribution of orders by hours of the day.
query4="""
select name,sum(od.quantity) from pizza_type pt
inner join pizza p on pt.pizza_type_id=p.pizza_type_id
inner join order_details od on p.pizza_id=od.pizza_id
group by name,price order by sum(od.quantity) desc
limit 1;
"""
most_common_pizza=execute_query(query4,conn)
print("Question4: ")
print(f"Mot Common Pizza:{most_common_pizza[0][0]} and it was ordered {most_common_pizza[0][1]} times")
print("")
'''
# Q8:   The category-wise distribution of pizzas.
query4="""
select name,sum(od.quantity) from pizza_type pt
inner join pizza p on pt.pizza_type_id=p.pizza_type_id
inner join order_details od on p.pizza_id=od.pizza_id
group by name,price order by sum(od.quantity) desc
limit 1;
"""
most_common_pizza=execute_query(query4,conn)
print("Question4: ")
print(f"Mot Common Pizza:{most_common_pizza[0][0]} and it was ordered {most_common_pizza[0][1]} times")
print("")

# Q9:   The average number of pizzas ordered per day.
query4="""
select name,sum(od.quantity) from pizza_type pt
inner join pizza p on pt.pizza_type_id=p.pizza_type_id
inner join order_details od on p.pizza_id=od.pizza_id
group by name,price order by sum(od.quantity) desc
limit 1;
"""
most_common_pizza=execute_query(query4,conn)
print("Question4: ")
print(f"Mot Common Pizza:{most_common_pizza[0][0]} and it was ordered {most_common_pizza[0][1]} times")
print("")

# Q10:   Top 3 most ordered pizza type base on revenue.
query4="""
select name,sum(od.quantity) from pizza_type pt
inner join pizza p on pt.pizza_type_id=p.pizza_type_id
inner join order_details od on p.pizza_id=od.pizza_id
group by name,price order by sum(od.quantity) desc
limit 1;
"""
most_common_pizza=execute_query(query4,conn)
print("Question4: ")
print(f"Mot Common Pizza:{most_common_pizza[0][0]} and it was ordered {most_common_pizza[0][1]} times")
print("")

# Q11:   The percentage contribution of each pizza type to revenue.
query4="""
select name,sum(od.quantity) from pizza_type pt
inner join pizza p on pt.pizza_type_id=p.pizza_type_id
inner join order_details od on p.pizza_id=od.pizza_id
group by name,price order by sum(od.quantity) desc
limit 1;
"""
most_common_pizza=execute_query(query4,conn)
print("Question4: ")
print(f"Mot Common Pizza:{most_common_pizza[0][0]} and it was ordered {most_common_pizza[0][1]} times")
print("")

# Q12:   The cumulative revenue generated over time.
query4="""
select name,sum(od.quantity) from pizza_type pt
inner join pizza p on pt.pizza_type_id=p.pizza_type_id
inner join order_details od on p.pizza_id=od.pizza_id
group by name,price order by sum(od.quantity) desc
limit 1;
"""
most_common_pizza=execute_query(query4,conn)
print("Question4: ")
print(f"Mot Common Pizza:{most_common_pizza[0][0]} and it was ordered {most_common_pizza[0][1]} times")
print("")

# Q13:   The top 3 most ordered pizza type based on revenue for each pizza category.
query4="""
select name,sum(od.quantity) from pizza_type pt
inner join pizza p on pt.pizza_type_id=p.pizza_type_id
inner join order_details od on p.pizza_id=od.pizza_id
group by name,price order by sum(od.quantity) desc
limit 1;
"""
most_common_pizza=execute_query(query4,conn)
print("Question4: ")
print(f"Mot Common Pizza:{most_common_pizza[0][0]} and it was ordered {most_common_pizza[0][1]} times")
print("")
# Close the cursor and connection
cur.close()
conn.close()
'''
