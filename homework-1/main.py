"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
import datetime

# заполняем таблицу employees
with open('north_data/employees_data.csv', 'r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='1234')
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)',
                                (row['employee_id'],
                                 row['first_name'],
                                 row['last_name'],
                                 row['title'],
                                 datetime.datetime.strptime(row['birth_date'], '%Y-%m-%d'),
                                 row['notes']))
        finally:
            conn.close()

# заполняем таблицу customers
with open('north_data/customers_data.csv', 'r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='1234')
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('INSERT INTO customers VALUES (%s, %s, %s)',
                                (row['customer_id'],
                                 row['company_name'],
                                 row['contact_name']))
        finally:
            conn.close()

# заполняем таблицу orders
with open('north_data/orders_data.csv', 'r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='1234')
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',
                                (row['order_id'],
                                 row['customer_id'],
                                 row['employee_id'],
                                 row['order_date'],
                                 row['ship_city']))
        finally:
            conn.close()
