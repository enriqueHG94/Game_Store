CREATE SCHEMA IF NOT EXISTS csv;

CREATE TABLE csv.customers (
  customer_id VARCHAR(255) PRIMARY KEY,
  customer_name VARCHAR(255),
  email VARCHAR(255),
  phone VARCHAR(255),
  country VARCHAR(255),
  city VARCHAR(255),
  address VARCHAR(255),
  postal_code VARCHAR(255),
  birth_date VARCHAR(255),
  gender VARCHAR(50),
  registration_date VARCHAR(255),
  purchase_history VARCHAR(255),
  loyalty_points VARCHAR(255),
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.stores (
  store_id VARCHAR(255) PRIMARY KEY,
  store_name VARCHAR(255),
  region VARCHAR(255),
  address VARCHAR(255),
  postal_code VARCHAR(255),
  city VARCHAR(255),
  phone VARCHAR(255),
  schedule TEXT,
  email_contact VARCHAR(255),
  opening_date VARCHAR(255),
  number_employees VARCHAR(255),
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.employees (
  employee_id VARCHAR(255) PRIMARY KEY,
  employee_name VARCHAR(255),
  role VARCHAR(255),
  store_id VARCHAR(255) REFERENCES csv.stores(store_id),
  email VARCHAR(255),
  phone VARCHAR(255),
  hire_date VARCHAR(255),
  salary VARCHAR(255),
  weekly_hours VARCHAR(255),
  birth_date VARCHAR(255),
  employee_address VARCHAR(255),
  social_security_number VARCHAR(255),
  nationality VARCHAR(255),
  region VARCHAR(255),
  postal_code VARCHAR(255),
  city VARCHAR(255),
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.payment_methods (
  payment_method_id VARCHAR(255) PRIMARY KEY,
  method_name VARCHAR(255),
  description TEXT,
  processing_fee VARCHAR(255),
  processing_time VARCHAR(255),
  available VARCHAR(50),
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.providers (
  provider_id VARCHAR(255) PRIMARY KEY,
  provider_name VARCHAR(255),
  contact_name VARCHAR(255),
  phone VARCHAR(255),
  email_contact VARCHAR(255),
  website VARCHAR(255),
  product_type VARCHAR(255),
  country_of_origin VARCHAR(255),
  start_date VARCHAR(255),
  payment_terms VARCHAR(255),
  city VARCHAR(255),
  provider_address VARCHAR(255),
  postal_code VARCHAR(255),
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.products (
  product_id VARCHAR(255) PRIMARY KEY,
  product_name VARCHAR(255),
  price VARCHAR(255),
  genre VARCHAR(255),
  platform VARCHAR(255),
  recommended_age VARCHAR(255),
  manufacturer VARCHAR(255),
  release_date VARCHAR(255),
  language VARCHAR(255),
  provider_id VARCHAR(255) REFERENCES csv.providers(provider_id),
  rating VARCHAR(255),
  weight VARCHAR(255),
  dimensions VARCHAR(255),
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.promotions (
  promotion_id VARCHAR(255) PRIMARY KEY,
  promotion_name VARCHAR(255),
  start_date VARCHAR(255),
  end_date VARCHAR(255),
  discount_type VARCHAR(255),
  discount_value VARCHAR(255),
  terms_and_conditions TEXT,
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.inventory (
  inventario_id VARCHAR(255) PRIMARY KEY,
  product_id VARCHAR(255) REFERENCES csv.products(product_id),
  store_id VARCHAR(255) REFERENCES csv.stores(store_id),
  available_stock VARCHAR(255),
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.sales (
  sale_id VARCHAR(255) PRIMARY KEY,
  customer_id VARCHAR(255) REFERENCES csv.customers(customer_id),
  employee_id VARCHAR(255) REFERENCES csv.employees(employee_id),
  store_id VARCHAR(255) REFERENCES csv.stores(store_id),
  sale_date VARCHAR(255),
  product_id VARCHAR(255) REFERENCES csv.products(product_id),
  quantity VARCHAR(255),
  payment_method_id VARCHAR(255) REFERENCES csv.payment_methods(payment_method_id),
  promotion_id VARCHAR(255) REFERENCES csv.promotions(promotion_id),
  comments TEXT,
  unit_price VARCHAR(255),
  total_price VARCHAR(255),
  load_timestamp TIMESTAMP
);

CREATE TABLE csv.shipments (
  shipping_id VARCHAR(255) PRIMARY KEY,
  sale_id VARCHAR(255) REFERENCES csv.sales(sale_id),
  customer_id VARCHAR(255) REFERENCES csv.customers(customer_id),
  shipping_address VARCHAR(255),
  shipping_company VARCHAR(255),
  shipping_cost VARCHAR(255),
  shipping_date VARCHAR(255),
  estimated_delivery_date VARCHAR(255),
  delivery_status VARCHAR(255),
  tracking_number VARCHAR(255),
  load_timestamp TIMESTAMP
);
