CREATE SCHEMA IF NOT EXISTS csv;

CREATE TABLE csv.dim_customers (
  customer_id VARCHAR(100) PRIMARY KEY,
  customer_name VARCHAR(100),
  email VARCHAR(100),
  phone VARCHAR(100),
  country VARCHAR(100),
  region VARCHAR (100),
  city VARCHAR(100),
  address VARCHAR(100),
  postal_code CHAR(5),
  birth_date DATE,
  age INTEGER,
  age_group VARCHAR(50),
  gender VARCHAR(50),
  registration_date DATE,
  purchase_history VARCHAR(100),
  loyalty_points INTEGER,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.dim_stores (
  store_id VARCHAR(100) PRIMARY KEY,
  store_name VARCHAR(100),
  phone VARCHAR(100),
  email_contact VARCHAR(100),
  number_employees INTEGER,
  country VARCHAR(100),
  region VARCHAR(100),
  city VARCHAR(100),
  address VARCHAR(100),
  postal_code CHAR(5),
  opening_date DATE,
  schedule TEXT,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.dim_employees (
  employee_id VARCHAR(100) PRIMARY KEY,
  store_id VARCHAR(100) REFERENCES csv.dim_stores(store_id),
  employee_name VARCHAR(100),
  role VARCHAR(100),
  email VARCHAR(100),
  phone VARCHAR(100),
  salary DECIMAL(10, 2),
  weekly_hours INTEGER,
  nationality VARCHAR(100),
  country VARCHAR(50),
  city VARCHAR(100),
  employee_address VARCHAR(100),
  postal_code CHAR(5),
  birth_date DATE,
  hire_date DATE,
  load_timestamp TIMESTAMP
);


CREATE TABLE csv.dim_payment_methods (
    payment_method_id VARCHAR(100) PRIMARY KEY,
    method_name VARCHAR(100),
    description TEXT,
    processing_fee FLOAT,
    processing_time VARCHAR(100),
    available BOOLEAN,
    load_timestamp TIMESTAMP
);

CREATE TABLE csv.dim_providers (
  provider_id VARCHAR(100) PRIMARY KEY,
  provider_name VARCHAR(100),
  contact_name VARCHAR(100),
  phone VARCHAR(100),
  email_contact VARCHAR(100),
  product_type VARCHAR(100),
  website VARCHAR(100),
  country_of_origin VARCHAR(100),
  city VARCHAR(100),
  provider_address VARCHAR(100),
  postal_code CHAR(5),
  payment_terms INTEGER,
  start_date DATE,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.dim_products (
  product_id VARCHAR(100) PRIMARY KEY,
  provider_id VARCHAR(100) REFERENCES csv.dim_providers(provider_id),
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

CREATE TABLE csv.dim_promotions (
  promotion_id VARCHAR(100) PRIMARY KEY,
  promotion_name VARCHAR(100),
  discount_type VARCHAR(100),
  terms_and_conditions TEXT,
  discount_value FLOAT,
  start_date DATE,
  end_date DATE,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.dim_time (
  time_id BIGINT PRIMARY KEY,
  date DATE,
  year INT,
  quarter INT,
  semester INT,
  month INT,
  month_name VARCHAR(100),
  week_of_year INT,
  week_of_month INT,
  day INT,
  day_name VARCHAR(100),
  day_of_week INT,
  day_of_year INT,
  is_weekend BOOLEAN,
  is_leap_year BOOLEAN
);

CREATE TABLE csv.fct_inventory (
  inventory_id VARCHAR(100) PRIMARY KEY,
  store_id VARCHAR(100) REFERENCES csv.dim_stores(store_id),
  product_id VARCHAR(100) REFERENCES csv.dim_products(product_id),
  available_stock INTEGER,
  low_stock_alert BOOLEAN,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.fact_sales (
  sale_id VARCHAR(100) PRIMARY KEY,
  customer_id VARCHAR(100) REFERENCES csv.dim_customers(customer_id),
  employee_id VARCHAR(100) REFERENCES csv.dim_employees(employee_id),
  store_id VARCHAR(100) REFERENCES csv.dim_stores(store_id),
  product_id VARCHAR(100) REFERENCES csv.dim_products(product_id),
  payment_method_id VARCHAR(100) REFERENCES csv.dim_payment_methods(payment_method_id),
  promotion_id VARCHAR(100) REFERENCES csv.dim_promotions(promotion_id),
  comments TEXT,
  quantity INTEGER,
  unit_price DECIMAL(10, 2),
  total_price DECIMAL(10, 2),
  sale_date TIMESTAMP,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.fct_shipments (
  shipping_id VARCHAR(100) PRIMARY KEY,
  sale_id VARCHAR(100) REFERENCES csv.fact_sales(sale_id),
  customer_id VARCHAR(100) REFERENCES csv.dim_customers(customer_id),
  shipping_address VARCHAR(100),
  shipping_company VARCHAR(100),
  delivery_status VARCHAR(100),
  tracking_number VARCHAR(100),
  shipping_cost DECIMAL(10, 2),
  shipping_date TIMESTAMP,
  estimated_delivery_date TIMESTAMP,
  load_timestamp TIMESTAMP
);
