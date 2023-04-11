import vertica_python
import datetime as dt

from datetime import timedelta
import psycopg2
from vertica_python import connect
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'schedule_interval': None
}

dag = DAG(
    'migrate_data',
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


hubs = [
    ('"Staging_Layer".categories', '"Core_Layer".h_categories', ['category_id']),
    ('"Staging_Layer".customers', '"Core_Layer".h_customers', ['customer_id']),
    ('"Staging_Layer".employees', '"Core_Layer".h_employees', ['employee_id']),
    ('"Staging_Layer".orders', '"Core_Layer".h_orders', ['order_id']),
    ('"Staging_Layer".products', '"Core_Layer".h_products', ['product_id']),
    ('"Staging_Layer".shippers', '"Core_Layer".h_shippers', ['shipper_id']),
    ('"Staging_Layer".suppliers', '"Core_Layer".h_suppliers', ['supplier_id']),
    ('"Staging_Layer".region', '"Core_Layer".h_regions', ['region_id']),
    ('"Staging_Layer".territories', '"Core_Layer".h_territories', ['territory_id']),
    ('"Staging_Layer".customer_demographics', '"Core_Layer".h_customer_type', ['customer_type_id']),
]

links = [
    ('"Staging_Layer".customer_customer_demo', '"Core_Layer".l_customer_types', ['customer_id', 'customer_type_id']),
    ('"Staging_Layer".products', '"Core_Layer".l_suppliers', ['supplier_id', 'product_id']),
    ('"Staging_Layer".orders','"Staging_Layer".order_details', '"Core_Layer".l_orders', ['"Staging_Layer".orders.order_id', 'customer_id', 'product_id'],['order_id', 'customer_id', 'product_id'],('order_id', 'order_id'), True),
    ('"Staging_Layer".orders','"Staging_Layer".shippers', '"Core_Layer".l_ship_order', ['"Staging_Layer".orders.order_id', '"Staging_Layer".orders.shipper_id'], ['order_id', 'shipper_id'],('shipper_id', 'shipper_id'),True),
    ('"Staging_Layer".products', '"Core_Layer".l_product_categories', ['product_id', 'category_id']),
    ('"Staging_Layer".orders', '"Core_Layer".l_employees_orders', ['order_id','employee_id']),
    ('"Staging_Layer".employee_territories', '"Core_Layer".l_employee_territories', ['employee_id', 'territory_id']),
    ('"Staging_Layer".territories', '"Core_Layer".l_territories_regions', ['territory_id', 'region_id']),
]

satellites = [
    ('"Staging_Layer".categories', '"Core_Layer".s_categories', ['category_id', 'category_name', 'description']),
    ('"Staging_Layer".customers', '"Core_Layer".s_customers', ['customer_id', 'company_name', 'contact_name', 'contact_title', 'address', 'city', 'region', 'postal_code', 'country', 'phone', 'fax']),
    ('"Staging_Layer".employees', '"Core_Layer".s_employees', ['employee_id', 'last_name', 'first_name', 'title', 'title_of_courtesy', 'birth_date', 'hire_date', 'address', 'city', 'region', 'postal_code', 'country', 'home_phone', 'extension', 'photo', 'notes', 'reports_to']),
    ('"Staging_Layer".products', '"Core_Layer".s_products', ['product_id', 'product_name', 'supplier_id', 'category_id', 'quantity_per_unit', 'unit_price', 'units_in_stock', 'units_on_order', 'reorder_level', 'discontinued']),
    ('"Staging_Layer".shippers', '"Core_Layer".s_shippers', ['shipper_id', 'company_name', 'phone']),
    ('"Staging_Layer".suppliers', '"Core_Layer".s_suppliers', ['supplier_id', 'company_name', 'contact_name', 'contact_title', 'address', 'city', 'region', 'postal_code', 'country', 'phone', 'fax', 'homepage']),
    ('"Staging_Layer".region', '"Core_Layer".s_regions', ['region_id', 'region_description']),
    ('"Staging_Layer".territories', '"Core_Layer".s_territories', ['territory_id', 'territory_description', 'region_id']),
    ('"Staging_Layer".customer_demographics', '"Core_Layer".s_customer_demographics', ['customer_type_id', 'customer_desc']),
    ('"Staging_Layer".order_details','"Staging_Layer".orders', '"Core_Layer".s_order_biz_details', ['"Staging_Layer".orders.order_id','customer_id', 'product_id', 'unit_price', 'quantity', 'discount'], ['order_id', 'customer_id','product_id', 'unit_price', 'quantity', 'discount'],('order_id', 'order_id'), True),
    ('"Staging_Layer".orders','"Staging_Layer".order_details','"Staging_Layer".products', '"Core_Layer".s_order_ship_details', ['"Staging_Layer".orders.order_id', 'customer_id', '"Staging_Layer".order_details.product_id', 'order_date', 'required_date', 'shipped_date', 'shipper_id', 'freight', 'ship_name', 'ship_address', 'ship_city', 'ship_region', 'ship_postal_code', 'ship_country'],['order_id', 'customer_id', 'product_id', 'order_date', 'required_date', 'shipped_date', 'shipper_id', 'freight', 'ship_name', 'ship_address', 'ship_city', 'ship_region', 'ship_postal_code', 'ship_country'],('order_id', 'order_id'),('product_id', 'product_id'), True),
    ('"Staging_Layer".products', '"Core_Layer".s_products', ['product_id', 'product_name', 'supplier_id', 'category_id', 'quantity_per_unit', 'unit_price', 'units_in_stock', 'units_on_order', 'reorder_level', 'discontinued']),
]
table_mappings = hubs + links + satellites

batch_size = 100000
record_source = 'source_system'

def transform_data(row, record_source):
    transformed_row = list(row)
    transformed_row.append(dt.datetime.now())
    transformed_row.append(record_source)
    return transformed_row

def migrate_data(source_table, dest_table, select, insert, join_on=None, join_on2=None, join_condition=None, source_table2=None, source_table3=None):
    with vertica_python.connect(**vertica_conn_info) as source_conn:
        if not join_on:
            unique_id_column = select[0]
        if join_on:
            unique_id_column = insert[0]
        dest_cursor = source_conn.cursor()
        dest_cursor.execute(f"SELECT MAX({unique_id_column}) FROM {dest_table}")
        max_unique_id = dest_cursor.fetchone()[0]
        
        # Modify the source query to include the WHERE clause for incremental loading
        source_query = f'SELECT {", ".join(select)} FROM {source_table}'
        join_sttmnt = f' WHERE {source_table}.{unique_id_column} > {max_unique_id if max_unique_id else 0} ORDER BY {unique_id_column} LIMIT {batch_size}'
        print(source_query)
        
        if join_on:
            join_on_source, join_on_dest = join_on
            source_query += f' JOIN {source_table2} ON {source_table}.{join_on_source} = {source_table2}.{join_on_dest}'
        if source_table3 and join_condition and join_on and join_on2:
            join_on_source2, join_on_dest2 = join_on2
            source_query += f' JOIN {source_table3} ON {source_table2}.{join_on_source2} = {source_table3}.{join_on_dest2}'
        source_query += join_sttmnt
        print(source_query)


        with source_conn.cursor() as source_cursor:
            
            source_cursor.execute(source_query)
            if source_table2 and join_condition and join_on:
                dest_query = f"INSERT INTO {dest_table} ({', '.join(insert + ['from_date', 'record_source'])}) VALUES ({', '.join(['%s'] * (len(select) + 2))})"
            else:
                dest_query = f"INSERT INTO {dest_table} ({', '.join(select + ['from_date', 'record_source'])}) VALUES ({', '.join(['%s'] * (len(select) + 2))})"
            with source_conn.cursor() as dest_cursor:
                batch = []
                print(dest_query)
                for row in source_cursor.iterate():
                    transformed_row = transform_data(row, record_source)
                    batch.append(transformed_row)

                    if len(batch) >= batch_size:
                        dest_cursor.executemany(dest_query, batch)
                        source_conn.commit()
                        batch = []

                if batch:
                    dest_cursor.executemany(dest_query, batch)
                    source_conn.commit()

                    
def migrate_all_data():
    for mapping in table_mappings:
        if len(mapping) == 3:
            source_table, dest_table, select = mapping
            insert = None
            join_condition = None
            source_table2 = None
            join_on = None
            join_on2 = None
            source_table3=None

        if len(mapping) == 7:
            source_table, source_table2, dest_table, select, insert, join_on, join_condition = mapping
            join_on2 = None
            source_table3=None
        if len(mapping) == 9:
            source_table, source_table2, source_table3, dest_table, select, insert, join_on, join_on2, join_condition = mapping
        migrate_data(source_table, dest_table, select, insert, join_on, join_on2, join_condition, source_table2, source_table3)


migrate_data_task = PythonOperator(
    task_id='migration',
    python_callable=migrate_all_data,
    dag=dag
)

migrate_all_data