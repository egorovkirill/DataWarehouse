import moment from 'moment';
import pg from 'pg';
const { Client } = pg;
import faker from 'faker';

const connectionString = `postgres://${encodeURIComponent('airflow')}:${encodeURIComponent('airflow')}@postgres:5432/airflow`;


async function getRandomValue(client, table, column) {
    const result = await client.query(`SELECT ${column} FROM ${table} ORDER BY RANDOM() LIMIT 1`);
    return result.rows.length > 0 ? result.rows[0][column] : null;
}


function setup() {
    const client = new Client({ connectionString });
    client.connect();
    return client;
  }


function generateCategoriesData() {
    return {
        category_name: faker.commerce.department(),
        description: faker.lorem.paragraph(),
        picture: faker.image.imageUrl(),
    };
}



function generateCustomersData() {
    return {
        company_name: faker.company.companyName(),
        contact_name: faker.name.findName(),
        contact_title: faker.name.jobTitle(),
        address: faker.address.streetAddress(),
        city: faker.address.city(),
        region: faker.address.state(),
        postal_code: faker.address.zipCode(),
        country: faker.address.country(),
        phone: faker.phone.phoneNumber(),
        fax: faker.phone.phoneNumber(),
    };
}

async function generateCustomerDemographicsData(client) {
    return {
        customer_desc: faker.lorem.paragraph(),
    };
}

async function generateCustomerCustomerDemoData(client) {
    return {
        customer_id: await getRandomValue(client, 'customers', 'customer_id'),
        customer_type_id: await getRandomValue(client, 'customer_demographics', 'customer_type_id'),
    };
}

async function generateEmployeesData(client) {
    const reports_to = await getRandomValue(client, 'employees', 'employee_id');
    return {
        last_name: faker.name.lastName(),
        first_name: faker.name.firstName(),
        title: faker.name.jobTitle(),
        title_of_courtesy: faker.name.prefix(),
        birth_date: moment(faker.date.past()).unix(),
        hire_date: moment(faker.date.past()).unix(),
        address: faker.address.streetAddress(),
        city: faker.address.city(),
        region: faker.address.state(),
        postal_code: faker.address.zipCode(),
        country: faker.address.country(),
        home_phone: faker.phone.phoneNumber(),
        extension: faker.datatype.number(4).toString(),
        photo: faker.image.image(),
        notes: faker.lorem.paragraph(),
        reports_to,
        photo_path: faker.image.imageUrl(),
    };
}


async function generateTerritoriesData(client) {
    return {
    territory_description: faker.address.streetName(),
    region_id: await getRandomValue(client, 'region', 'region_id'),
    };
    }

async function generateEmployeeTerritoriesData(client) {
    return {
        employee_id: await getRandomValue(client, 'employees', 'employee_id'),
        territory_id: await getRandomValue(client, 'territories', 'territory_id'),
    };
}


async function generateOrdersData(client) {
    return {
        customer_id: await getRandomValue(client, 'customers', 'customer_id'),
        employee_id: await getRandomValue(client, 'employees', 'employee_id'),
        order_date: Math.floor(Date.now() / 1000),
        required_date: moment(faker.date.soon()).unix(),
        shipped_date:  moment(faker.date.soon()).unix(),
        ship_via: await getRandomValue(client, 'shippers', 'shipper_id'),
        freight: faker.datatype.number(),
        ship_name: faker.company.companyName(),
        ship_address: faker.address.streetAddress(),
        ship_city: faker.address.city(),
        ship_region: faker.address.state(),
        ship_postal_code: faker.address.zipCode(),
        ship_country: faker.address.country(),
    };
}

async function generateOrderDetailsData(client) {
    return {
        product_id: await getRandomValue(client, 'products', 'product_id'),
        unit_price: faker.datatype.number(),
        quantity: faker.datatype.number({min: 1, max:5}),
        discount: faker.datatype.number(),
    };
}


async function generateProductsData(client) {
    return {
        product_name: faker.commerce.productName(),
        supplier_id: await getRandomValue(client, 'suppliers', 'supplier_id'),
        category_id: await getRandomValue(client, 'categories', 'category_id'),
        quantity_per_unit: faker.commerce.product(),
        unit_price: faker.commerce.price(),
        units_in_stock: faker.datatype.number(),
        units_on_order: faker.datatype.number(),
        reorder_level: faker.datatype.number(),
        discontinued: faker.datatype.boolean() ? 1 : 0,
};
}


function generateRegionData() {
    return {
        region_description: faker.address.state(),
    };
}

function generateShippersData() {
    return {
        company_name: faker.company.companyName(),
        phone: faker.phone.phoneNumber(),
    };
}

function generateSuppliersData() {
    return {
        company_name: faker.company.companyName(),
        contact_name: faker.name.findName(),
        contact_title: faker.name.jobTitle(),
        address: faker.address.streetAddress(),
        city: faker.address.city(),
        region: faker.address.state(),
        postal_code: faker.address.zipCode(),
        country: faker.address.country(),
        phone: faker.phone.phoneNumber(),
        fax: faker.phone.phoneNumber(),
        homepage: faker.internet.url(),
    };
}



function generateUsStatesData() {
    return {
        state_name: faker.address.state(),
        state_abbr: faker.address.stateAbbr(),
    };
}


async function defaultFunction(client) {

    const categoriesData = Array.from({ length: 40 }, generateCategoriesData);
        for (const category of categoriesData) {
            const values = Object.values(category);
            await client.query(
                'INSERT INTO categories (category_name, description, picture) VALUES ($1, $2, $3)',
                values
            );
        };


    
    const statesData = Array.from({ length: 50 }, generateUsStatesData);
    for (const states of statesData) {
        const values = Object.values(states);
        await client.query(
            'INSERT INTO us_states (state_name, state_abbr) VALUES ($1, $2)',
            values
        );
    };

    const regionData = Array.from({ length: 100 }, generateRegionData);
        for (const region of regionData) {
            const values = Object.values(region);
            await client.query(
                'INSERT INTO region (region_description) VALUES ($1)',
                values
            );
        };


    const territoriesData = Array.from({ length: 1000 }, () => generateTerritoriesData(client));
    for (const territories of await Promise.all(territoriesData)) {
        const values = Object.values(territories);
        await client.query(
            'INSERT INTO territories (territory_description, region_id) VALUES ($1, $2)',
            values
        );
    };

    const suppliersData = Array.from({ length: 100 }, generateSuppliersData);
    for (const suppliers of suppliersData) {
        const values = Object.values(suppliers);
        await client.query(
            'INSERT INTO suppliers (company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
            values
        );
    };

    const shippersData = Array.from({ length: 100 }, generateShippersData);
    for (const shippers of shippersData) {
        const values = Object.values(shippers);
        await client.query(
            'INSERT INTO shippers (company_name, phone) VALUES ($1, $2)',
            values
        );
    };

    const productsData = Array.from({ length: 1000 }, () => generateProductsData(client));
        for (const products of await Promise.all(productsData)) {
            const values = Object.values(products);
            await client.query(
                'INSERT INTO products (product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
                values
            );
        };


    // Insert fake data into customers table
    const customersData = Array.from({ length: 100 }, generateCustomersData);
    for (const customers of customersData) {
        const values = Object.values(customers);
    await client.query(
        'INSERT INTO customers (company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)',
        values
        );
    };

    // Insert fake data into customer_demographics table
    const customerDemographicsData = Array.from({ length: 100 }, () => generateCustomerDemographicsData(client));
    for (const customer_demograph of await Promise.all(customerDemographicsData)) {
        const values = Object.values(customer_demograph);
        await client.query(
            'INSERT INTO customer_demographics (customer_desc) VALUES ($1)',
            values
        );
    };
    
    // Insert fake data into customer_customer_demo table
    const customerCustomerDemoData = Array.from({ length: 100 }, () => generateCustomerCustomerDemoData(client));
    for (const customer_demo of await Promise.all(customerCustomerDemoData)) {
    const values = Object.values(customer_demo);
    await client.query(
        'INSERT INTO customer_customer_demo (customer_id, customer_type_id) VALUES ($1, $2)',
        values
    );
};

    

    // Insert fake data into employees table
    const employeesData = Array.from({ length: 100 }, () => generateEmployeesData(client));
    for (const employees of await Promise.all(employeesData)) {
    const values = Object.values(employees);
    await client.query(
        'INSERT INTO employees (last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, photo, notes, reports_to, photo_path) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)',
        values
    );
};


        // Insert fake data into employee_territories table
    const employeeTerritoriesData = Array.from({ length: 100 }, () => generateEmployeeTerritoriesData(client));
    for (const employeesTer of await Promise.all(employeeTerritoriesData)) {
        const values = Object.values(employeesTer);
        await client.query(
            'INSERT INTO employee_territories (employee_id, territory_id) VALUES ($1, $2)',
            values
        );
    };


while (true) {
    const orders = Array.from({ length: 1 }, () => generateOrdersData(client));
    const orderPromises = orders.map(async (order) => {
      const values = Object.values(await order);
      await client.query('BEGIN');
      try {
        const orderResult = await client.query(
          'INSERT INTO orders (customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING order_id',
          values
        );
        const orderId = orderResult.rows[0].order_id;

        const ordersData = Array.from({ length: 1 }, () => generateOrderDetailsData(client));
        const orderDetailsPromises = ordersData.map(async (orderDetails) => {
          const orderDetailsValues = [orderId, ...Object.values(await orderDetails)];
          await client.query(
            'INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES ($1, $2, $3, $4, $5)',
            orderDetailsValues
          );
        });
        await Promise.all(orderDetailsPromises);
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        console.error('Transaction error:', error);
      }
    });
    await Promise.all(orderPromises);
  }
}

async function teardown(client) {
    await client.end();
    }


const client = setup();
(async () => {
    await defaultFunction(client);
})().finally(() => {
    teardown(client);
});