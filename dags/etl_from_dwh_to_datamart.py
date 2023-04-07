from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import clickhouse_driver
import vertica_python

vertica_conn_info = {
        'host': 'vertica',
        'port': 5433,
        'user': 'dbadmin',
    }

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    'vertica_to_clickhouse_dm',
    default_args=default_args,
    description='Vertica -> clickhouse',
    schedule_interval=None,
    max_active_runs=1)

def load_data_to_clickhouse():
    with clickhouse_driver.dbapi.connect(host="clickhouse", port=9000, database="default", user="clickhouse", password="changeme") as conn:
        clickhouse_cursor = conn.cursor()
        clickhouse_cursor.execute("SELECT max(order_id) FROM dm_orders")
        last_order_id = clickhouse_cursor.fetchone()[0] or 0
        print(last_order_id)
        batch_size = 100000
        with vertica_python.connect(**vertica_conn_info) as source_conn:
            vertica_cur = source_conn.cursor()
            vertica_cur.execute("""
                SELECT (Core_Layer.s_order_ship_details.order_id, Core_Layer.s_order_ship_details.customer_id, Core_Layer.s_order_ship_details.product_id, product_name, Core_Layer.l_product_categories.category_id, category_name, order_date, required_date, shipped_date, shipper_id, freight, Core_Layer.s_order_biz_details.unit_price, quantity, discount, company_name,customer_type_id, Core_Layer.l_territories_regions.region_id,Core_Layer.l_territories_regions.territory_id,Core_Layer.l_employees_orders.employee_id, first_name, last_name)
                FROM Core_Layer.s_order_biz_details
                JOIN Core_Layer.s_products ON Core_Layer.s_order_biz_details.product_id = Core_Layer.s_products.product_id
                JOIN Core_Layer.s_order_ship_details ON Core_Layer.s_order_biz_details.order_id = Core_Layer.s_order_ship_details.order_id
                JOIN Core_Layer.l_product_categories ON Core_Layer.s_order_biz_details.product_id = Core_Layer.l_product_categories.product_id
                JOIN Core_Layer.s_categories ON Core_Layer.l_product_categories.category_id = Core_Layer.s_categories.category_id
                JOIN Core_Layer.s_customers ON Core_Layer.s_customers.customer_id = Core_Layer.s_order_biz_details.customer_id
                JOIN Core_Layer.l_employees_orders ON Core_Layer.l_employees_orders.order_id = Core_Layer.s_order_ship_details.order_id
                JOIN Core_Layer.l_customer_types ON Core_Layer.l_customer_types.customer_id = Core_Layer.s_order_biz_details.customer_id
                JOIN Core_Layer.s_employees ON Core_Layer.l_employees_orders.employee_id = Core_Layer.s_employees.employee_id
                JOIN Core_Layer.l_employee_territories ON Core_Layer.l_employee_territories.employee_id = Core_Layer.s_employees.employee_id
                JOIN Core_Layer.s_territories ON Core_Layer.s_territories.territory_id = Core_Layer.l_employee_territories.territory_id 
                JOIN Core_Layer.l_territories_regions ON Core_Layer.l_employee_territories.territory_id = Core_Layer.l_territories_regions.territory_id
                WHERE Core_Layer.s_order_ship_details.order_id > %s
                ORDER BY Core_Layer.s_order_ship_details.order_id
                LIMIT %s
                """, (last_order_id, batch_size ))            
                
            
            insert_query = "INSERT INTO dm_orders (order_id, customer_id, product_id, product_name, category_id, category_name, order_date, required_date, shipped_date, shipper_id, freight, unit_price, quantity, discount, company_name, customer_type_id, region_id, territory_id, employee_id, first_name, last_name) VALUES"
            
            while True:
                rows = [row[0] for row in vertica_cur.fetchall()]
                if not rows:
                    break

                clickhouse_cursor.executemany(insert_query, rows)
                conn.commit()

load_data_task = PythonOperator(
    task_id='load_data_to_clickhouse',
    python_callable=load_data_to_clickhouse,
    dag=dag
)
