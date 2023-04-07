from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import clickhouse_driver



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    'initialize_clickhouse_dm_orders_table',
    default_args=default_args,
    description='Create dm_orders table in ClickHouse',
    schedule_interval=None,
)



def create_dm_orders_table():
    conn = clickhouse_driver.dbapi.connect(host="clickhouse", port=9000, database="default",user="clickhouse",password="changeme")
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS dm_orders (order_id Int32, customer_id Int32, product_id Int32, product_name String, category_id Int32, category_name String, order_date String, required_date String, shipped_date String, shipper_id Int32, freight Float32, unit_price Float32, quantity Int32, discount Float32, company_name String, customer_type_id Int32, region_id Int32, territory_id Int32, employee_id Int32, first_name String, last_name String ) ENGINE = MergeTree() PARTITION BY order_id % 100 ORDER BY order_id;')
    


create_dm_orders_table_operator = PythonOperator(
    task_id='create_dm_orders_table',
    python_callable=create_dm_orders_table,
    dag=dag
)