CREATE TABLE categories (
    category_id int NOT NULL GENERATED ALWAYS AS IDENTITY,
    category_name character varying(100) NOT NULL,
    description text,
    picture text
);

CREATE TABLE customer_customer_demo (
    customer_id int NOT NULL,
    customer_type_id int NOT NULL
);

CREATE TABLE customer_demographics (
    customer_type_id int GENERATED ALWAYS AS IDENTITY NOT NULL,
    customer_desc text
);


CREATE TABLE customers (
    customer_id int NOT NULL GENERATED ALWAYS AS IDENTITY,
    company_name character varying(40) NOT NULL,
    contact_name character varying(100),
    contact_title character varying(100),
    address text,
    city character varying(100),
    region character varying(100),
    postal_code character varying(100),
    country character varying(100),
    phone character varying(24),
    fax character varying(24)
);


CREATE TABLE employees (
    employee_id int GENERATED ALWAYS AS IDENTITY NOT NULL,
    last_name character varying(200) NOT NULL,
    first_name character varying(200) NOT NULL,
    title character varying(200),
    title_of_courtesy character varying(25),
    birth_date character varying(15),
    hire_date character varying(15),
    address character varying(60),
    city character varying(200),
    region character varying(200),
    postal_code character varying(200),
    country character varying(200),
    home_phone character varying(24),
    extension character varying(4),
    photo character varying(255),
    notes text,
    reports_to int,
    photo_path character varying(255)
);




CREATE TABLE employee_territories (
    employee_id int NOT NULL,
    territory_id int NOT NULL
);






CREATE TABLE order_details (
    order_id int NOT NULL,
    product_id int NOT NULL,
    unit_price real NOT NULL,
    quantity int NOT NULL,
    discount real NOT NULL
);



CREATE TABLE orders (
    order_id int GENERATED ALWAYS AS IDENTITY NOT NULL,
    customer_id int NOT NULL,
    employee_id int,
    order_date character varying(15),
    required_date character varying(15),
    shipped_date character varying(15),
    ship_via int,
    freight real,
    ship_name character varying(100),
    ship_address character varying(60),
    ship_city character varying(100),
    ship_region character varying(100),
    ship_postal_code character varying(100),
    ship_country character varying(100)
);



CREATE TABLE products (
    product_id int GENERATED ALWAYS AS IDENTITY NOT NULL,
    product_name character varying(40) NOT NULL,
    supplier_id int,
    category_id int,
    quantity_per_unit character varying(20),
    unit_price real,
    units_in_stock int,
    units_on_order int,
    reorder_level int,
    discontinued integer NOT NULL
);






CREATE TABLE shippers (
    shipper_id int GENERATED ALWAYS AS IDENTITY NOT NULL,
    company_name character varying(40) NOT NULL,
    phone character varying(24)
);



CREATE TABLE suppliers (
    supplier_id int GENERATED ALWAYS AS IDENTITY NOT NULL,
    company_name character varying(40) NOT NULL,
    contact_name character varying(100),
    contact_title character varying(100),
    address character varying(60),
    city character varying(100),
    region character varying(100),
    postal_code character varying(100),
    country character varying(100),
    phone character varying(24),
    fax character varying(24),
    homepage text
);

CREATE TABLE region (
    region_id int GENERATED ALWAYS AS IDENTITY NOT NULL,
    region_description character varying(60) NOT NULL
);


CREATE TABLE territories (
    territory_id int GENERATED ALWAYS AS IDENTITY NOT NULL,
    territory_description character varying(60) NOT NULL,
    region_id int NOT NULL
);


CREATE TABLE us_states (
    state_id int GENERATED ALWAYS AS IDENTITY NOT NULL,
    state_name character varying(100),
    state_abbr character varying(2)
);


ALTER TABLE ONLY categories
    ADD CONSTRAINT pk_categories PRIMARY KEY (category_id);


ALTER TABLE ONLY customer_demographics
    ADD CONSTRAINT pk_customer_demographics PRIMARY KEY (customer_type_id);


ALTER TABLE ONLY customers
    ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);


ALTER TABLE ONLY employees
    ADD CONSTRAINT pk_employees PRIMARY KEY (employee_id);


ALTER TABLE ONLY orders
    ADD CONSTRAINT pk_orders PRIMARY KEY (order_id);


ALTER TABLE ONLY products
    ADD CONSTRAINT pk_products PRIMARY KEY (product_id);


ALTER TABLE ONLY region
    ADD CONSTRAINT pk_region PRIMARY KEY (region_id);



ALTER TABLE ONLY shippers
    ADD CONSTRAINT pk_shippers PRIMARY KEY (shipper_id);

ALTER TABLE ONLY suppliers
    ADD CONSTRAINT pk_suppliers PRIMARY KEY (supplier_id);


ALTER TABLE ONLY territories
    ADD CONSTRAINT pk_territories PRIMARY KEY (territory_id);


ALTER TABLE ONLY us_states
    ADD CONSTRAINT pk_usstates PRIMARY KEY (state_id);

ALTER TABLE ONLY orders
    ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES customers;


ALTER TABLE ONLY orders
    ADD CONSTRAINT fk_orders_employees FOREIGN KEY (employee_id) REFERENCES employees;

ALTER TABLE ONLY orders
    ADD CONSTRAINT fk_orders_shippers FOREIGN KEY (ship_via) REFERENCES shippers;


ALTER TABLE ONLY order_details
    ADD CONSTRAINT fk_order_details_products FOREIGN KEY (product_id) REFERENCES products;


ALTER TABLE ONLY order_details
    ADD CONSTRAINT fk_order_details_orders FOREIGN KEY (order_id) REFERENCES orders;

ALTER TABLE ONLY products
    ADD CONSTRAINT fk_products_categories FOREIGN KEY (category_id) REFERENCES categories;

ALTER TABLE ONLY products
    ADD CONSTRAINT fk_products_suppliers FOREIGN KEY (supplier_id) REFERENCES suppliers;


ALTER TABLE ONLY territories
    ADD CONSTRAINT fk_territories_region FOREIGN KEY (region_id) REFERENCES region;

ALTER TABLE ONLY employee_territories
    ADD CONSTRAINT fk_employee_territories_territories FOREIGN KEY (territory_id) REFERENCES territories;


ALTER TABLE ONLY employee_territories
    ADD CONSTRAINT fk_employee_territories_employees FOREIGN KEY (employee_id) REFERENCES employees;

ALTER TABLE ONLY customer_customer_demo
    ADD CONSTRAINT fk_customer_customer_demo_customer_demographics FOREIGN KEY (customer_type_id) REFERENCES customer_demographics;

ALTER TABLE ONLY customer_customer_demo
    ADD CONSTRAINT fk_customer_customer_demo_customers FOREIGN KEY (customer_id) REFERENCES customers;

ALTER TABLE ONLY employees
    ADD CONSTRAINT fk_employees_employees FOREIGN KEY (reports_to) REFERENCES employees;
