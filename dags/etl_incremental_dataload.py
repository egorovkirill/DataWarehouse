from datetime import timedelta
import psycopg2
from vertica_python import connect
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'postgres_to_vertica',
    default_args=default_args,
    description='Load data from PostgreSQL to Vertica',
    schedule_interval=timedelta(seconds=10),
    start_date=days_ago(0),
    catchup=False,
    max_active_runs=1
)

vertica_conn_info = {
        'host': 'vertica',
        'port': 5433,
        'user': 'dbadmin',
    }

tables = ['categories', 'customer_customer_demo', 'customer_demographics', 'customers', 'employees', 'employee_territories', 'products', 'shippers', 'suppliers', 'region', 'territories','orders', 'order_details']
unique_id_columns = {
'categories': 'category_id',
'customer_customer_demo': 'customer_id',
'customer_demographics': 'customer_type_id',
'customers': 'customer_id',
'employees': 'employee_id',
'employee_territories': 'employee_id',
'order_details': 'order_id',
'orders': 'order_id',
'products': 'product_id',
'shippers': 'shipper_id',
'suppliers': 'supplier_id',
'region': 'region_id',
'territories': 'territory_id',
}



def load_data_from_postgres_to_vertica():
    batch_size = 100000
    # ...
    postgres_conn_info = {
        'host': 'postgres',
        'dbname': 'airflow',
        'user': 'airflow',
        'password': 'airflow',
        'port': '5432',
    }



    # Connect to the PostgreSQL database
    with psycopg2.connect(**postgres_conn_info) as pg_conn:
        pg_cur = pg_conn.cursor()

        with connect(**vertica_conn_info) as vert_conn:
            vert_cur = vert_conn.cursor()

            for table in tables:
                unique_id_column = unique_id_columns[table]

                vert_cur.execute(f"SELECT MAX({unique_id_column}) FROM Staging_Layer.{table};")
                max_id = vert_cur.fetchone()[0]

                if max_id is None:
                    max_id = 0

                
                pg_cur.execute(f"SELECT * FROM {table} WHERE {unique_id_column} > {max_id} ORDER BY {unique_id_column} LIMIT {batch_size};")
                data = pg_cur.fetchall()

                if not data:
                    continue

                columns = [desc[0] for desc in pg_cur.description]
                insert_query = f"INSERT INTO Staging_Layer.{table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))});"

                vert_cur.executemany(insert_query, data)
                vert_conn.commit()

                max_id = data[-1][0]
                        

load_data_task = PythonOperator(
    task_id='load_data_from_postgres_to_vertica',
    python_callable=load_data_from_postgres_to_vertica,
    dag=dag
)

load_data_task