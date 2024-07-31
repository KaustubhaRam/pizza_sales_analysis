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
SELECT size, COUNT(*) AS size_count
FROM order_details od
JOIN pizza p ON od.pizza_id = p.pizza_id
GROUP BY size
ORDER BY size_count DESC
LIMIT 1;
"""
most_common_pizza=execute_query(query4,conn)
print("Question4: ")
print(f"Most Common Pizza:{most_common_pizza[0][0]} and it was ordered {most_common_pizza[0][1]} times")
print("")

# Q5:   The top 5 most ordered pizza types along their quantities.
query5="""
select pt.pizza_type_id,sum(od.quantity) from pizza_type pt
inner join pizza p on pt.pizza_type_id=p.pizza_type_id
inner join order_details od on p.pizza_id=od.pizza_id
group by pt.pizza_type_id order by sum(od.quantity) desc
limit 5;
"""
top5_most_common_pizza=execute_query(query5,conn)
print("Question5: ")
print("5 Most Common Pizza: ")
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
quantity_category=execute_query(query6,conn)
print("Question6: ")
for x in quantity_category:
    print(f"{x[0]}: {x[1]}")
print("")

# Q7:   The distribution of orders by hours of the day.
query7="""
select EXTRACT(HOUR FROM time::time) as hour,count(*) from
order_details od inner join orders o on od.order_id=od.order_details_id
group by hour order by hour;
"""
hourly_distribution=execute_query(query7,conn)
print("Question7: ")
for x in hourly_distribution:
    print(f"{x[0]} hours: {x[1]} orders")
print("")

# Q8:   The category-wise distribution of pizzas.
query8="""
select pt.pizza_type_id,count(*) from pizza_type pt
inner join pizza p on pt.pizza_type_id=p.pizza_type_id
inner join order_details od on p.pizza_id=od.pizza_id
group by pt.pizza_type_id ;
"""
quantity_category=execute_query(query8,conn)
print("Question8: ")
for x in quantity_category:
    print(f"{x[0]} was ordered {x[1]} times")
print("")


# Q9:   The average number of pizzas ordered per day.
query9 = """
SELECT AVG(daily_orders.total_quantity) AS avg_pizzas_per_day
FROM (
    SELECT date, SUM(od.quantity) AS total_quantity
    FROM order_details od
    JOIN orders o ON od.order_id = o.order_id
    GROUP BY date
) AS daily_orders;
"""
average_pizzas_per_day=execute_query(query9,conn)
print("Question9: ")
print("Average number of pizzas ordered per day:", average_pizzas_per_day[0][0])
print("")

# Q10:   Top 3 most ordered pizza type base on revenue.
query10="""
SELECT pt.pizza_type_id, SUM(od.quantity * p.price) AS total_revenue
FROM order_details od
JOIN pizza p ON od.pizza_id = p.pizza_id
JOIN pizza_type pt ON p.pizza_type_id = pt.pizza_type_id
GROUP BY pt.pizza_type_id
ORDER BY total_revenue DESC
LIMIT 3;
"""
top3_most_ordered_pizza=execute_query(query10,conn)
print("Question10: ")
print("3 Most ordered pizza type base on revenue: ")
for x in top3_most_ordered_pizza:
    print(f"{x[0]}: {x[1]}")
print("")


# Q11:   The percentage contribution of each pizza type to revenue.
query11="""
select pt.pizza_type_id,(sum(od.quantity * p.price)/(select sum(od.quantity*p.price)
from order_details od  inner join pizza p on p.pizza_id=od.pizza_id)*100) as revenue_percentage
FROM order_details od
JOIN pizza p ON od.pizza_id = p.pizza_id
JOIN pizza_type pt ON p.pizza_type_id = pt.pizza_type_id
GROUP BY pt.pizza_type_id ORDER BY revenue_percentage DESC;
"""
percentage_contribution=execute_query(query11,conn)
print("Question11: ")
print("Percentage contribution of each pizza: ")
for x in percentage_contribution:
    print(f"{x[0]}: {round(x[1],2)}%")
print("")

# Q12:   The cumulative revenue generated over time.
query12="""
SELECT date, 
       SUM(daily_revenue) OVER (ORDER BY date) AS cumulative_revenue
FROM (
    SELECT o.date, SUM(od.quantity * p.price) AS daily_revenue
    FROM order_details od
    JOIN pizza p ON od.pizza_id = p.pizza_id
    JOIN orders o ON od.order_id = o.order_id
    GROUP BY o.date
) AS daily_revenue_totals
ORDER BY date;
"""
cumulative_revenue_over_time = execute_query(query12, conn)
print("Question12: ")
print("Cumulative revenue generated over time:")
for x in cumulative_revenue_over_time:
    print(f"{x[0]}: {round(x[1],2)}")
print("")

# Q13:   The top 3 most ordered pizza type based on revenue for each pizza category.
query13="""
select category,total_revenue from
(select pt.category,sum(od.quantity*p.price) as total_revenue,
row_number() over (partition by pt.category order by sum(od.quantity*p.price)desc) as rank
from order_details od
inner join pizza p on od.pizza_id=p.pizza_id
inner join pizza_type pt on p.pizza_type_id=pt.pizza_type_id
group by pt.category)
as ranked_pizzas
where rank<=3
order by total_revenue desc
limit 3;
"""
top_3_pizza_types_by_category=execute_query(query13,conn)
print("Question13: ")
print("Top 3 most ordered pizza types based on revenue for each pizza category:")
for x in top_3_pizza_types_by_category:
    print(f"{x[0]}: {x[1]}")
print("")
# Close the cursor and connection
cur.close()
conn.close()
