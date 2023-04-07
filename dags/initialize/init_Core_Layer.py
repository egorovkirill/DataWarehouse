import datetime as dt
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
import vertica_python
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 3, 30),
    'retries': 0,
    'schedule_interval': None
}

# Hubs
hubs = '''CREATE TABLE IF NOT EXISTS "Core_Layer".h_categories (
category_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
PRIMARY KEY (category_id)
);


CREATE TABLE IF NOT EXISTS "Core_Layer".h_customer_type (
customer_type_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
PRIMARY KEY (customer_type_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".h_customers (
customer_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".h_employees (
employee_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
PRIMARY KEY (employee_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".h_orders (
order_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".h_products (
product_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".h_shippers (
shipper_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
PRIMARY KEY (shipper_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".h_suppliers (
supplier_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
PRIMARY KEY (supplier_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".h_regions (
region_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
PRIMARY KEY (region_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".h_territories (
territory_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
PRIMARY KEY (territory_id)
);
'''


### Links

links = '''
CREATE TABLE IF NOT EXISTS "Core_Layer".l_customer_types (
customer_id int NOT NULL,
customer_type_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
CONSTRAINT pk_customer_types PRIMARY KEY (customer_id, customer_type_id),
CONSTRAINT fk_customer_types_customers FOREIGN KEY (customer_id) REFERENCES "Core_Layer".h_customers(customer_id),
CONSTRAINT fk_customer_types_customer_type FOREIGN KEY (customer_type_id) REFERENCES "Core_Layer".h_customer_type(customer_type_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".l_suppliers(
supplier_id int NOT NULL,
product_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
CONSTRAINT pk_suppliers PRIMARY KEY (supplier_id, product_id),
CONSTRAINT fk_suppliers_suppliers FOREIGN KEY (supplier_id) REFERENCES "Core_Layer".h_suppliers(supplier_id),
CONSTRAINT fk_suppliers_products FOREIGN KEY (product_id) REFERENCES "Core_Layer".h_products(product_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".l_orders(
order_id int NOT NULL,
customer_id int NOT NULL,
product_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
CONSTRAINT pk_orders PRIMARY KEY (order_id, customer_id, product_id),
CONSTRAINT fk_orders_order FOREIGN KEY (order_id) REFERENCES "Core_Layer".h_orders(order_id),
CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES "Core_Layer".h_customers(customer_id),
CONSTRAINT fk_orders_products FOREIGN KEY (product_id) REFERENCES "Core_Layer".h_products(product_id)
);


CREATE TABLE IF NOT EXISTS "Core_Layer".l_ship_order(
order_id int NOT NULL,
shipper_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
CONSTRAINT pk_ship_order PRIMARY KEY (order_id, shipper_id),
CONSTRAINT fk_ship_order_orders FOREIGN KEY (order_id) REFERENCES "Core_Layer".h_orders(order_id),
CONSTRAINT fk_ship_order_shippers FOREIGN KEY (shipper_id) REFERENCES "Core_Layer".h_shippers(shipper_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".l_product_categories(
product_id int NOT NULL,
category_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
CONSTRAINT pk_product_categories PRIMARY KEY (product_id, category_id),
CONSTRAINT fk_product_categories_products FOREIGN KEY (product_id) REFERENCES "Core_Layer".h_products(product_id),
CONSTRAINT fk_product_categories_categories FOREIGN KEY (category_id) REFERENCES "Core_Layer".h_categories(category_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".l_employees_orders(
employee_id int NOT NULL,
order_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
CONSTRAINT pk_employees_orders PRIMARY KEY (employee_id, order_id),
CONSTRAINT fk_employees_orders_employees FOREIGN KEY (employee_id) REFERENCES "Core_Layer".h_employees(employee_id),
CONSTRAINT fk_employees_orders_orders FOREIGN KEY (order_id) REFERENCES "Core_Layer".h_orders(order_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".l_employee_territories (
employee_id int NOT NULL,
territory_id int NOT NULL,
from_date timestamp NOT NULL,
to_date timestamp,
record_source varchar(255) NOT NULL,
CONSTRAINT pk_employee_territories PRIMARY KEY (employee_id, territory_id),
CONSTRAINT fk_employee_territories_employees FOREIGN KEY (employee_id) REFERENCES "Core_Layer".h_employees(employee_id),
CONSTRAINT fk_employee_territories_territories FOREIGN KEY (territory_id) REFERENCES "Core_Layer".h_territories(territory_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".l_territories_regions(
territory_id int NOT NULL,
region_id int NOT NULL,
from_date timestamp NOT NULL,
record_source varchar(255) NOT NULL,
CONSTRAINT pk_territories_regions PRIMARY KEY (territory_id, region_id),
CONSTRAINT fk_territories_regions_territories FOREIGN KEY (territory_id) REFERENCES "Core_Layer".h_territories(territory_id),
CONSTRAINT fk_territories_regions_regions FOREIGN KEY (region_id) REFERENCES "Core_Layer".h_regions(region_id)
);
'''

# Satelites

sat = '''
CREATE TABLE IF NOT EXISTS "Core_Layer".s_categories (
category_id int NOT NULL,
from_date timestamp NOT NULL,
to_date timestamp,
category_name varchar(100) NOT NULL,
description varchar(65000),
picture varchar(65000),
record_source varchar(255) NOT NULL,
CONSTRAINT pk_s_categories PRIMARY KEY (category_id),
CONSTRAINT fk_s_categories_h_categories FOREIGN KEY (category_id) REFERENCES "Core_Layer".h_categories(category_id)
);



CREATE TABLE IF NOT EXISTS "Core_Layer".s_customer_demographics (
customer_type_id int NOT NULL,
from_date timestamp NOT NULL,
to_date timestamp,
customer_desc varchar(65000),
record_source varchar(255) NOT NULL,
CONSTRAINT pk_customer_demographics PRIMARY KEY (customer_type_id),
CONSTRAINT fk_customer_demographics_h_customer_type FOREIGN KEY (customer_type_id) REFERENCES "Core_Layer".h_customer_type(customer_type_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".s_customers (
customer_id int NOT NULL,
from_date timestamp NOT NULL,
to_date timestamp,
company_name varchar(40) NOT NULL,
contact_name varchar(100),
contact_title varchar(100),
address varchar(65000),
city varchar(100),
region varchar(100),
postal_code varchar(100),
country varchar(100),
phone varchar(24),
fax varchar(24),
record_source varchar(255) NOT NULL,
CONSTRAINT pk_s_customers PRIMARY KEY (customer_id),
CONSTRAINT fk_s_customers_h_customers FOREIGN KEY (customer_id) REFERENCES "Core_Layer".h_customers(customer_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".s_employees (
employee_id int NOT NULL,
from_date timestamp NOT NULL,
to_date timestamp,
last_name varchar(200) NOT NULL,
first_name varchar(200) NOT NULL,
title varchar(200),
title_of_courtesy varchar(25),
birth_date varchar(15),
hire_date varchar(15),
address varchar(60),
city varchar(200),
region varchar(200),
postal_code varchar(200),
country varchar(200),
home_phone varchar(24),
extension varchar(4),
photo varchar(255),
notes varchar(65000),
reports_to int,
photo_path varchar(255),
record_source varchar(255) NOT NULL,
CONSTRAINT pk_s_employees PRIMARY KEY (employee_id),
CONSTRAINT fk_s_employees_h_employees FOREIGN KEY (employee_id) REFERENCES "Core_Layer".h_employees(employee_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".s_order_biz_details (
order_id int NOT NULL,
customer_id int NOT NULL,
product_id int NOT NULL,
from_date timestamp NOT NULL,
to_date timestamp,
unit_price real NOT NULL,
quantity int NOT NULL,
discount real NOT NULL,
record_source varchar(255) NOT NULL,
CONSTRAINT pk_s_order_biz_details PRIMARY KEY (order_id),
CONSTRAINT fk_s_order_biz_details_l_orders FOREIGN KEY (order_id, customer_id, product_id) REFERENCES "Core_Layer".l_orders(order_id, customer_id, product_id)
);


CREATE TABLE IF NOT EXISTS "Core_Layer".s_order_ship_details (
order_id int NOT NULL,
customer_id int NOT NULL,
product_id int NOT NULL,
from_date timestamp NOT NULL,
to_date timestamp,
order_date varchar(15),
required_date varchar(15),
shipped_date varchar(15),
shipper_id int,
freight real,
ship_name varchar(100),
ship_address varchar(60),
ship_city varchar(100),
ship_region varchar(100),
ship_postal_code varchar(100),
ship_country varchar(100),
record_source varchar(255) NOT NULL,
CONSTRAINT pk_s_order_ship_details PRIMARY KEY (order_id),
CONSTRAINT fk_s_order_ship_details_l_orders FOREIGN KEY (order_id, customer_id, product_id) REFERENCES "Core_Layer".l_orders(order_id, customer_id, product_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".s_products (
product_id int NOT NULL,
from_date timestamp NOT NULL,
to_date timestamp,
product_name varchar(40) NOT NULL,
supplier_id int,
category_id int,
quantity_per_unit varchar(20),
unit_price real,
units_in_stock int,
units_on_order int,
reorder_level int,
discontinued integer NOT NULL,
record_source varchar(255) NOT NULL,
CONSTRAINT pk_s_products PRIMARY KEY (product_id),
CONSTRAINT fk_s_products_h_products FOREIGN KEY (product_id) REFERENCES "Core_Layer".h_products(product_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".s_shippers (
shipper_id int NOT NULL,
from_date timestamp NOT NULL,
to_date timestamp,
company_name varchar(40) NOT NULL,
phone varchar(24),
record_source varchar(255) NOT NULL,
CONSTRAINT pk_s_shippers PRIMARY KEY (shipper_id),
CONSTRAINT fk_s_shippers_h_shippers FOREIGN KEY (shipper_id) REFERENCES "Core_Layer".h_shippers(shipper_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".s_suppliers (
supplier_id int NOT NULL,
from_date timestamp NOT NULL,
to_date timestamp,
company_name varchar(40) NOT NULL,
contact_name varchar(100),
contact_title varchar(100),
address varchar(60),
city varchar(100),
region varchar(100),
postal_code varchar(100),
country varchar(100),
phone varchar(24),
fax varchar(24),
homepage varchar(65000),
record_source varchar(255) NOT NULL,
CONSTRAINT pk_s_suppliers PRIMARY KEY (supplier_id),
CONSTRAINT fk_s_suppliers_h_suppliers FOREIGN KEY (supplier_id) REFERENCES "Core_Layer".h_suppliers(supplier_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".s_regions (
region_id int NOT NULL,
from_date timestamp NOT NULL,
to_date timestamp,
region_description varchar(60) NOT NULL,
record_source varchar(255) NOT NULL,
CONSTRAINT pk_s_regions PRIMARY KEY (region_id),
CONSTRAINT fk_s_regions_h_regions FOREIGN KEY (region_id) REFERENCES "Core_Layer".h_regions(region_id)
);

CREATE TABLE IF NOT EXISTS "Core_Layer".s_territories (
territory_id int NOT NULL,
from_date timestamp NOT NULL,
to_date timestamp,
territory_description varchar(60) NOT NULL,
region_id int NOT NULL,
record_source varchar(255) NOT NULL,
CONSTRAINT pk_s_territories PRIMARY KEY (territory_id),
CONSTRAINT fk_s_territories_h_territories FOREIGN KEY (territory_id) REFERENCES "Core_Layer".h_territories(territory_id)
);
'''

tables=[hubs,links,sat]
def create_schema_if_not_exists(**kwargs):
    schema_name = 'Core_Layer'
    vertica_conn_info = {
        'host': 'vertica',
        'port': 5433,
        'user': 'dbadmin',
    }

    with vertica_python.connect(**vertica_conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM v_catalog.schemata WHERE schema_name='{schema_name}'")
        schema_exists = cur.fetchone()[0]

        if not schema_exists:
            cur.execute(f'CREATE SCHEMA "{schema_name}"')
            conn.commit()
        conn.close()

def create_tables_in_vertica(**kwargs):
    vertica_conn_info = {
        'host': 'vertica',
        'port': 5433,
        'user': 'dbadmin',
    }
    with vertica_python.connect(**vertica_conn_info) as conn:
            cur = conn.cursor()

            for table in tables:
                cur.execute(table)
                conn.commit()
            conn.close()
   

with DAG('init_core_layer',
default_args=default_args,
schedule_interval='@once',
catchup=False,
max_active_runs=1) as dag:
    
    create_schema_if_not_exists = PythonOperator(
    task_id='create_schema_if_not_exists',
    python_callable=create_schema_if_not_exists,
    provide_context=True
    )

    create_tables_in_vertica = PythonOperator(
    task_id='create_tables_in_vertica',
    python_callable=create_tables_in_vertica,
    provide_context=True
    )

create_schema_if_not_exists >> create_tables_in_vertica