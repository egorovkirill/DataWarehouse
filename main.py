import psycopg2
import faker

conn = psycopg2.connect(
    host="localhost",
    database="airflow",
    user="airflow",
    password="airflow",
    port=5436
)

def generate_categories_data():
    return {
        "category_id": faker.random.number(),
        "category_name": faker.commerce.department(),
        "description": faker.lorem.paragraph(),
        "picture": faker.image_url(),
    }

def generate_customer_customer_demo_data():
    return {
        "customer_id": faker.random_alphaNumeric(5).upper(),
        "customer_type_id": faker.random_number(),
    }

def generate_customer_demographics_data():
    return {
        "customer_type_id": faker.random_number(),
        "customer_desc": faker.lorem.paragraph(),
    }

def generate_customers_data():
    return {
        "customer_id": faker.random_alphaNumeric(5).upper(),
        "company_name": faker.company.company_name(),
        "contact_name": faker.name.find_name(),
        "contact_title": faker.name.job_title(),
        "address": faker.address.street_address(),
        "city": faker.address.city(),
        "region": faker.address.state(),
        "postal_code": faker.address.zip_code(),
        "country": faker.address.country(),
        "phone": faker.phone_number(),
        "fax": faker.phone_number(),
    }

def generate_employees_data():
    return {
        "employee_id": faker.random_number(),
        "last_name": faker.name.last_name(),
        "first_name": faker.name.first_name(),
        "title": faker.name.job_title(),
        "title_of_courtesy": faker.name.prefix(),
        "birth_date": faker.date_of_birth(),
        "hire_date": faker.date_between(start_date='-10y', end_date='today'),
        "address": faker.address.street_address(),
        "city": faker.address.city(),
        "region": faker.address.state(),
        "postal_code": faker.address.zip_code(),
        "country": faker.address.country(),
        "home_phone": faker.phone_number(),
        "extension": faker.random_number(digits=4),
        "photo": faker.image_url(),
        "notes": faker.lorem.paragraph(),
        "reports_to": faker.random_number(),
        "photo_path": faker.image_url(),
    }

def generate_employee_territories_data():
    return {
        "employee_id": faker.random_number(),
        "territory_id": faker.random_alphaNumeric(20).upper(),
    }

def generate_orders_data():
    return {
        "order_id": faker.random_number(),
        "customer_id": faker.random_alphaNumeric(5).upper(),
        "employee_id": faker.random_number(),
        "order_date": faker.date_between(start_date='-10y', end_date='today'),
        "required_date": faker.date_between(start_date='-10y', end_date='today'),
        "shipped_date": faker.date_between(start_date='-10y', end_date='today'),
        "ship_via": faker.random_number(),
        "freight": faker.random_number(),
        "ship_name": faker.company.company_name(),
        "ship_address": faker.address.street_address(),
        "ship_city": faker.address.city(),
        "ship_region": faker.address.state(),
        "ship_postal_code": faker.address.zip_code(),
        "ship_country": faker.address.country(),
    }

def generate_order_details_data():
    return {
        "order_id": faker.random_number(),
        "product_id": faker.random_number(),
        "unit_price": faker.random_number(),
        "quantity": faker.random_number(),
        "discount": faker.random_number(),
    }

def generate_products_data():
    return {
        "product_id": faker.random_number(),
        "product_name": faker.commerce.product_name(),
        "supplier_id": faker.random_number(),
        "category_id": faker.random_number(),
        "quantity_per_unit": faker.commerce.product(),
        "unit_price": faker.random_number(),
        "units_in_stock": faker.random_number(),
        "units_on_order": faker.random_number(),
        "reorder_level": faker.random_number(),
        "discontinued": faker.random_boolean(),
}

def generate_region_data():
    return {
    "region_id": faker.random_number(),
    "region_description": faker.address.state(),
    }

def generate_shippers_data():
    return {
    "shipper_id": faker.random_number(),
    "company_name": faker.company.company_name(),
    "phone": faker.phone_number(),
    }

def generate_suppliers_data():
    return {
    "supplier_id": faker.random_number(),
    "company_name": faker.company.company_name(),
    "contact_name": faker.name.find_name(),
    "contact_title": faker.name.job_title(),
    "address": faker.address.street_address(),
    "city": faker.address.city(),
    "region": faker.address.state(),
    "postal_code": faker.address.zip_code(),
    "country": faker.address.country(),
    "phone": faker.phone_number(),
    "fax": faker.phone_number(),
    "homepage": faker.internet.url(),
    }

def generate_territories_data():
    return {
    "territory_id": faker.random_alphaNumeric(20).upper(),
    "territory_description": faker.address.street_name(),
    "region_id": faker.random_number(),
    }

def generate_us_states_data():
    return {
    "state_id": faker.random_number(),
    "state_name": faker.address.state(),
    "state_abbr": faker.address.state_abbr(),
    "state_region": faker.address.region(),
    }

def generate_data(insert_query, generate_data_func):
    with conn.cursor() as cur:
        for _ in range(100):
            data = [tuple(generate_data_func().values()) for _ in range(100)]
            cur.executemany(insert_query, data)
            conn.commit()

while True:
    generate_data("INSERT INTO categories (category_id, category_name, description, picture) VALUES (%s, %s, %s, %s)", generate_categories_data)
    generate_data("INSERT INTO customer_customer_demo (customer_id, customer_type_id) VALUES (%s, %s)", generate_customer_customer_demo_data)
    generate_data("INSERT INTO customer_demographics (customer_type_id, customer_desc) VALUES (%s, %s)", generate_customer_demographics_data)
    generate_data("INSERT INTO customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", generate_customers_data)
    generate_data("INSERT INTO employees (employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, photo, notes, reports_to, photo_path) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", generate_employees_data)
    generate_data("INSERT INTO employee_territories (employee_id, territory_id) VALUES (%s, %s)", generate_employee_territories_data)
    generate_data("INSERT INTO orders (order_id, customer_id, employee_id, order_date, required_date, shipped_date,ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", generate_orders_data)
    generate_data("INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES (%s, %s, %s, %s, %s)", generate_order_details_data)
    generate_data("INSERT INTO products (product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", generate_products_data)
    generate_data("INSERT INTO regions (region_id, region_description) VALUES (%s, %s)", generate_region_data)
    generate_data("INSERT INTO shippers (shipper_id, company_name, phone) VALUES (%s, %s, %s)", generate_shippers_data)
    generate_data("INSERT INTO suppliers (supplier_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", generate_suppliers_data)
    generate_data("INSERT INTO territories (territory_id, territory_description, region_id) VALUES (%s, %s, %s)", generate_territories_data)
    generate_data("INSERT INTO us_states (state_id, state_name, state_abbr, state_region) VALUES (%s, %s, %s, %s)", generate_us_states_data)

conn.close()