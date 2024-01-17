CREATE SCHEMA IF NOT EXISTS csv;

CREATE TABLE csv.trf_customers (
  customer_id VARCHAR(100) PRIMARY KEY,
  customer_name VARCHAR(100),
  email VARCHAR(100),
  phone VARCHAR(100),
  country VARCHAR(100),
  city VARCHAR(100),
  address VARCHAR(100),
  postal_code CHAR(5),
  birth_date DATE,
  age INTEGER,
  gender VARCHAR(50),
  registration_date DATE,
  purchase_history VARCHAR(100),
  loyalty_points INTEGER,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.trf_stores (
  store_id VARCHAR(100) PRIMARY KEY,
  store_name VARCHAR(100),
  region VARCHAR(100),
  address VARCHAR(100),
  postal_code CHAR(5),
  city VARCHAR(100),
  phone VARCHAR(100),
  schedule TEXT,
  email_contact VARCHAR(100),
  opening_date DATE,
  number_employees INTEGER,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.trf_employees (
  employee_id VARCHAR(100) PRIMARY KEY,
  store_id VARCHAR(100) REFERENCES csv.trf_stores(store_id),
  employee_name VARCHAR(100),
  role VARCHAR(100),
  email VARCHAR(100),
  phone VARCHAR(100),
  salary INTEGER,
  weekly_hours INTEGER,
  nationality VARCHAR(100),
  region VARCHAR(100),
  city VARCHAR(100),
  employee_address VARCHAR(100),
  postal_code VARCHAR(100),
  social_security_number VARCHAR(11),
  birth_date DATE,
  hire_date DATE,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.trf_payment_methods (
    payment_method_id VARCHAR(100) PRIMARY KEY,
    method_name VARCHAR(100),
    description TEXT,
    processing_fee FLOAT,
    processing_time VARCHAR(100),
    available BOOLEAN,
    load_timestamp TIMESTAMP
);

CREATE TABLE csv.trf_providers (
  provider_id VARCHAR(100) PRIMARY KEY,
  provider_name VARCHAR(100),
  contact_name VARCHAR(100),
  email_contact VARCHAR(100),
  phone VARCHAR(100),
  provider_address VARCHAR(100),
  postal_code VARCHAR(100),
  city VARCHAR(100),
  country_of_origin VARCHAR(100),
  product_type VARCHAR(100),
  payment_terms INTEGER,
  website VARCHAR(100),
  start_date DATE,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.trf_products (
  product_id VARCHAR(100) PRIMARY KEY,
  provider_id VARCHAR(100) REFERENCES csv.trf_providers(provider_id),
  product_name VARCHAR(100),
  genre VARCHAR(100),
  platform VARCHAR(100),
  manufacturer VARCHAR(100),
  languages VARCHAR(100),
  recommended_age VARCHAR(100),
  price FLOAT,
  rating FLOAT,
  weight VARCHAR(100),
  dimensions VARCHAR(100),
  release_date DATE,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.trf_promotions (
  promotion_id VARCHAR(100) PRIMARY KEY,
  promotion_name VARCHAR(100),
  discount_type VARCHAR(100),
  terms_and_conditions TEXT,
  discount_value FLOAT,
  start_date DATE,
  end_date DATE,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.trf_inventory (
  inventory_id VARCHAR(100) PRIMARY KEY,
  store_id VARCHAR (100) REFERENCES csv.trf_stores(store_id),
  product_id VARCHAR(100) REFERENCES csv.trf_products(product_id),
  available_stock INTEGER,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.trf_sales (
  sale_id VARCHAR(100) PRIMARY KEY,
  customer_id VARCHAR(100) REFERENCES csv.trf_customers(customer_id),
  employee_id VARCHAR(100) REFERENCES csv.trf_employees(employee_id),
  store_id VARCHAR(100) REFERENCES csv.trf_stores(store_id),
  product_id VARCHAR(100) REFERENCES csv.trf_products(product_id),
  payment_method_id VARCHAR(100) REFERENCES csv.trf_payment_methods(payment_method_id),
  promotion_id VARCHAR(100) REFERENCES csv.trf_promotions(promotion_id),
  comments TEXT,
  quantity INTEGER,
  unit_price DECIMAL(10, 2),
  total_price DECIMAL(10, 2),
  sale_date TIMESTAMP,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.trf_shipments (
  shipping_id VARCHAR(100) PRIMARY KEY,
  sale_id VARCHAR(100) REFERENCES csv.trf_sales(sale_id),
  customer_id VARCHAR(100) REFERENCES csv.trf_customers(customer_id),
  shipping_address VARCHAR(100),
  shipping_company VARCHAR(100),
  delivery_status VARCHAR(100),
  tracking_number VARCHAR(100),
  shipping_cost DECIMAL(10, 2),
  shipping_date TIMESTAMP,
  estimated_delivery_date TIMESTAMP,
  load_timestamp TIMESTAMP
);
