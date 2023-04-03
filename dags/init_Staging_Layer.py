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

def extract_data_from_postgres(**kwargs):
    postgres_credentials = {
        'host': 'postgres',
        'dbname': 'airflow',
        'user': 'airflow',
        'password': 'airflow',
        'port': '5432',
    }
    conn = psycopg2.connect(**postgres_credentials)
    cursor = conn.cursor()
    query = "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name=%s"
    table_list = ['categories','customer_customer_demo','customer_demographics','customers','employees','orders','order_details','employee_territories','products','shippers','suppliers','region','territories']
    extracted_data = {}
    for table in table_list:
        cursor.execute(query, (table,))
        columns = cursor.fetchall()
        extracted_data[table] = {
            'columns': {col_name: (data_type, char_max_length) for col_name, data_type, char_max_length in columns}
        }
    conn.close()
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)

table_schemas = {
    'categories': [
        ('category_id', 'INTEGER', None),
        ('category_name', 'VARCHAR', 100),
        ('description', 'VARCHAR', 65000),
        ('picture', 'VARCHAR', 65000),
    ],
    'customer_customer_demo': [
        ('customer_id', 'INTEGER', None),
        ('customer_type_id', 'INTEGER', None),
    ],
    'customer_demographics': [
        ('customer_type_id', 'INTEGER', None),
        ('customer_desc', 'VARCHAR', 65000),
    ],
    'customers': [
        ('customer_id', 'INTEGER', None),
        ('company_name', 'VARCHAR', 40),
        ('contact_name', 'VARCHAR', 100),
        ('contact_title', 'VARCHAR', 100),
        ('address', 'VARCHAR', 65000),
        ('city', 'VARCHAR', 100),
        ('region', 'VARCHAR', 100),
        ('postal_code', 'VARCHAR', 100),
        ('country', 'VARCHAR', 100),
        ('phone', 'VARCHAR', 24),
        ('fax', 'VARCHAR', 24),
    ],
    'employees': [
        ('employee_id', 'INTEGER', None),
        ('last_name', 'VARCHAR', 200),
        ('first_name', 'VARCHAR', 200),
        ('title', 'VARCHAR', 200),
        ('title_of_courtesy', 'VARCHAR', 25),
        ('birth_date', 'VARCHAR', 15),
        ('hire_date', 'VARCHAR', 15),
        ('address', 'VARCHAR', 60),
        ('city', 'VARCHAR', 200),
        ('region', 'VARCHAR', 200),
        ('postal_code', 'VARCHAR', 200),
        ('country', 'VARCHAR', 200),
        ('home_phone', 'VARCHAR', 24),
        ('extension', 'VARCHAR', 4),
        ('photo', 'VARCHAR', 255),
        ('notes', 'VARCHAR', 65000),
        ('reports_to', 'INTEGER', None),
        ('photo_path', 'VARCHAR', 255),
    ],
    'employee_territories': [
        ('employee_id', 'INTEGER', None),
        ('territory_id', 'INTEGER', None),
    ],
    'order_details': [
        ('order_id', 'INTEGER', None),
        ('product_id', 'INTEGER', None),
        ('unit_price', 'FLOAT', None),
        ('quantity', 'INTEGER', None),
        ('discount', 'FLOAT', None),
    ],
    'orders': [
        ('order_id', 'INTEGER', None),
        ('customer_id', 'INTEGER', None),
        ('employee_id', 'INTEGER', None),
        ('order_date', 'VARCHAR', 15),
        ('required_date', 'VARCHAR', 15),
        ('shipped_date', 'VARCHAR', 15),
        ('shipper_id', 'INTEGER', None),
        ('freight', 'FLOAT', None),
        ('ship_name', 'VARCHAR', 100),
        ('ship_address', 'VARCHAR', 60),
        ('ship_city', 'VARCHAR', 100),
        ('ship_region', 'VARCHAR', 100),
        ('ship_postal_code', 'VARCHAR', 100),
        ('ship_country', 'VARCHAR', 100),
    ],
    'products': [
        ('product_id', 'INTEGER', None),
        ('product_name', 'VARCHAR', 40),
        ('supplier_id', 'INTEGER', None),
        ('category_id', 'INTEGER', None),
        ('quantity_per_unit', 'VARCHAR', 20),
        ('unit_price', 'FLOAT', None),
        ('units_in_stock', 'INTEGER', None),
        ('units_on_order', 'INTEGER', None),
        ('reorder_level', 'INTEGER', None),
        ('discontinued', 'INTEGER', None),
    ],
    'region': [
        ('region_id', 'INTEGER', None),
        ('region_description', 'VARCHAR', 50),
    ],
    'shippers': [
        ('shipper_id', 'INTEGER', None),
        ('company_name', 'VARCHAR', 40),
        ('phone', 'VARCHAR', 24),
    ],
    'suppliers': [
        ('supplier_id', 'INTEGER', None),
        ('company_name', 'VARCHAR', 40),
        ('contact_name', 'VARCHAR', 100),
        ('contact_title', 'VARCHAR', 100),
        ('address', 'VARCHAR', 65000),
        ('city', 'VARCHAR', 100),
        ('region', 'VARCHAR', 100),
        ('postal_code', 'VARCHAR', 100),
        ('country', 'VARCHAR', 100),
        ('phone', 'VARCHAR', 24),
        ('fax', 'VARCHAR', 24),
        ('homepage', 'VARCHAR', 65000),
    ],
    'territories': [
        ('territory_id', 'INTEGER', None),
        ('territory_description', 'VARCHAR', 60),
        ('region_id', 'INTEGER', None),
    ],
}
         
def create_schema_if_not_exists(**kwargs):
    schema_name = 'Staging_Layer'
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

def map_data_types(pg_type, length=None):
    data_type_map = {
        'int': ('INTEGER', None),
        'real': ('FLOAT', None),
        'character varying': ('VARCHAR', 400),
        'text': ('VARCHAR', 65000),
    }

    return data_type_map.get(pg_type, ('VARCHAR', 400))

def create_tables_in_vertica(**kwargs):
    vertica_conn_info = {
        'host': 'vertica',
        'port': 5433,
        'user': 'dbadmin',
    }

    with vertica_python.connect(**vertica_conn_info) as conn:
        cur = conn.cursor()

        for table, columns in table_schemas.items():
            # Build the CREATE TABLE statement
            create_table_stmt = f'CREATE TABLE IF NOT EXISTS "Staging_Layer"."{table}" (\n'
            column_definitions = []
            for col_name, col_type, col_length in columns:
                vertica_col_type = col_type
                if col_type == 'VARCHAR' and col_length is not None:
                    vertica_col_type += f'({col_length})'
                column_definitions.append(f'"{col_name}" {vertica_col_type}')
            create_table_stmt += ',\n'.join(column_definitions) + '\n);'

            # Execute the statement
            cur.execute(create_table_stmt)
            conn.commit()
        conn.close()


with DAG('init_staging_layer',
default_args=default_args,
schedule_interval='@once',
catchup=False,
max_active_runs=1) as dag:
    extract_data_from_postgres = PythonOperator(
    task_id='extract_data_from_postgres',
    python_callable=extract_data_from_postgres,
    provide_context=True
    )

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

extract_data_from_postgres >> create_schema_if_not_exists >> create_tables_in_vertica