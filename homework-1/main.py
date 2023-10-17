import csv
import psycopg2
import os

db_conn_pw = os.getenv("db_conn_pw")

# connect to db
connection = psycopg2.connect(host="localhost", database='north', user='postgres',
                              password=db_conn_pw)

employees_data = "north_data/employees_data.csv"
customers_data = "north_data/customers_data.csv"
orders_data = "north_data/orders_data.csv"

try:
    with connection:
        with connection.cursor() as cur:
            # execution query
            with open(employees_data) as emp_csv:
                emp_reader = csv.DictReader(emp_csv)
                for row in emp_reader:
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                (row["employee_id"], row["first_name"], row["last_name"],
                                 row["title"], row["birth_date"], row["notes"]))

            with open(customers_data) as cust_csv:
                cust_reader = csv.DictReader(cust_csv)
                for row in cust_reader:
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                (row["customer_id"], row["company_name"],
                                 row["contact_name"]))

            with open(orders_data) as ord_csv:
                ord_reader = csv.DictReader(ord_csv)
                for row in ord_reader:
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                (row["order_id"], row["customer_id"], row["employee_id"],
                                 row["order_date"], row["ship_city"]))

finally:
    connection.close()
