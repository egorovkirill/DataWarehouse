import datetime as dt
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import vertica_python

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 3, 29),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5)
}

def extract_data_from_postgres(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='1234124124124142')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = 'SELECT * FROM "{table_name}"'
    table_list = ['categories','customer_customer_demo','customer_demographics','customers','employees','employee_territories','products','shippers','suppliers','region','territories','us_states']
    extracted_data = {}
    fetch_size = 10000
    for table in table_list:
        cursor.execute(query.format(table_name=table))
        columns = {desc[0]: (desc[1], desc[3]) for desc in cursor.description}

        data_chunks = []
        while True:
            rows = cursor.fetchmany(fetch_size)
            if not rows:
                break

            df = pd.DataFrame(rows, columns=[col for col in columns])
            data_chunks.append(df.to_dict(orient='records'))

        extracted_data[table] = {
            'data': [record for chunk in data_chunks for record in chunk],
            'columns': columns
        }
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)

def create_schema_if_not_exists(**kwargs):
    schema_name = 'Data Layer'
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
            create_table_stmt = f'CREATE TABLE IF NOT EXISTS "Data Layer"."{table}" (\n'
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


def load_data_to_vertica(**kwargs):
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data')
    vertica_conn_info = {
        'host': 'vertica',
        'port': 5433,
        'user': 'dbadmin',
    }

    with vertica_python.connect(**vertica_conn_info) as conn:
        cur = conn.cursor()

        for table, table_data in extracted_data.items():
            rows = table_data['data']
            columns = table_data['columns']

            if len(rows) > 0:
                column_names = ', '.join(['"' + col + '"' for col in columns])
                placeholders = ', '.join(['%s'] * len(columns))
                insert_query = f'INSERT INTO "Data Layer"."{table}" ({column_names}) VALUES ({placeholders})'
                
                for row in rows:
                    values = [row[col] for col in columns]
                    cur.execute(insert_query, tuple(values))

                conn.commit()

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

    load_data_to_vertica = PythonOperator(
    task_id='load_data_to_vertica',
    python_callable=load_data_to_vertica,
    provide_context=True
    )

extract_data_from_postgres >> create_schema_if_not_exists >> create_tables_in_vertica >>  load_data_to_vertica