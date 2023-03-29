import datetime as dt
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
import vertica_python

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 3, 29),
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
    query = "SELECT column_name FROM information_schema.columns WHERE table_name=%s"
    table_list = ['categories','customer_customer_demo','customer_demographics','customers','employees','orders','order_details','employee_territories','products','shippers','suppliers','region','territories','us_states']
    extracted_data = {}
    for table in table_list:
        cursor.execute(query, (table,))
        columns = [desc[0] for desc in cursor.fetchall()]
        extracted_data[table] = {
            'columns': {col_name: None for col_name in columns}
        }
    conn.close()
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)

def create_schema_if_not_exists(**kwargs):
    schema_name = 'Staging Area'
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
        'text': ('VARCHAR', 65535),
    }

    return data_type_map.get(pg_type, ('VARCHAR', 400))

def create_tables_in_vertica(**kwargs):
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data')
    vertica_conn_info = {
        'host': 'vertica',
        'port': 5433,
        'user': 'dbadmin',
    }

    with vertica_python.connect(**vertica_conn_info) as conn:
        cur = conn.cursor()

        for table, table_data in extracted_data.items():
            columns = table_data['columns']

            # Build the CREATE TABLE statement
            create_table_stmt = f'CREATE TABLE IF NOT EXISTS "Staging Area"."{table}" (\n'
            column_definitions = []
            for col_name, col_type_length in columns.items():
                if col_type_length is not None:
                    col_type, col_length = map_data_types(col_type_length[0], col_type_length[1])
                    vertica_col_type = col_type
                    if col_type == 'VARCHAR' and col_length is not None:
                        vertica_col_type += f'({col_length})'
                    column_definitions.append(f'"{col_name}" {vertica_col_type}')
                else:
                    # Handle missing data type by setting a default data type
                    column_definitions.append(f'"{col_name}" VARCHAR(255)')
            create_table_stmt += ',\n'.join(column_definitions) + '\n);'

            # Execute the statement
            cur.execute(create_table_stmt)
            conn.commit()
        conn.close()


with DAG('etl_postgres_to_vertica',
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