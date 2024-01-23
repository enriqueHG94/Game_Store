# ELT Project with PySpark and Snowflake

## Contents
- [General Description](#general-description)
- [Initial Setup](#initial-setup)
- [Project Structure](#project-structure)
- [Bronze Zone](#bronze-zone)
- [Silver Zone](#silver-zone)
- [Gold Zone](#gold-zone)
- [Use Cases](#use-cases)
- [Conclusion](#conclusion)

## General Description
This ELT (Extract, Load, and Transform) project uses Python 3.10.0, PySpark 3.3.3, and Snowflake to process and analyze large datasets. Following a medallion architecture, the project divides data into Bronze, Silver, and Gold zones, optimizing each stage of the process for efficiency and effective analysis. The project focuses on data analysis for a game store, particularly in inventory management, sales, and shipping.

## Initial Setup
The project begins with setting up the development environment, including installing Python and PySpark, and configuring Snowflake credentials in a `credentials.py` file. This approach ensures the security of sensitive data and the effective integration of the utilized tools.

## Project Structure
The project is organized into several folders reflecting the different phases of the ELT process:
- `bronze`: Contains the original CSV files, representing the data in its rawest state.
- `silver`: Includes PySpark scripts for transforming the Bronze data, preparing it for more detailed analysis.
- `gold`: Hosts scripts for creating the dimensional model in Snowflake, facilitating advanced analysis and report generation.
- `jars`: Contains the necessary dependencies for PySpark integration with Snowflake, crucial for data connection and manipulation.

## Bronze Zone
At this stage, raw data is loaded directly from CSV files into Snowflake using `extract_and_load.py`. This process ensures that the data remains in its original form before any transformation, preserving its integrity.

Execution Order:
1. `tables_bronze.sql`: Creates tables in Snowflake for each CSV.
2. `extract_and_load.py`: Loads the data into Snowflake.

## Silver Zone
Here, Bronze data is transformed and normalized for analytical use. Using scripts like `trf_customers.py` and `trf_stores.py`, the data is cleaned, additional metrics are calculated, and reformatted to enhance its utility. Transformations include standardizing formats such as phone numbers and dates, and deriving new columns to enrich the datasets.

Execution Order:
1. Execute `tables_silver.sql` to create table structures in the Silver zone.
2. Execute PySpark scripts (e.g., `trf_customers.py`) for each dataset, transforming and loading the data into Silver tables.
3. Execute `new_row.sql` to add a new promotion to the promotions table.

## Gold Zone
The final phase involves creating a dimensional model, structuring the data for efficient analysis. Scripts like `dim_stores.py` and `dim_employees.py` are used to load data into this model, creating a suitable environment for business analysis and decision-making.

Execution Order:
1. `tables_gold.sql` to create table structures in the Gold zone.
2. PySpark scripts (e.g., `dim_stores.py`, `dim_employees.py`) to load data into the Gold tables.

## Use Cases
Specific use cases were developed to demonstrate the applicability of the transformed data in the Gold zone:
- **Low Inventory Analysis (`fct_inventory`)**: Identifies products with low stock in stores, aiding in preventing shortages.
- **Sales Analysis (`fct_sales`)**: Examines total sales volume, best-selling products, and employee sales performance, providing valuable insights into the effectiveness of sales strategies.
- **Shipping Analysis (`fct_shipments`)**: Assesses the efficiency of shipping companies and analyzes the costs and punctuality of shipments.

## Conclusion
This project demonstrates efficient use of Python, PySpark, and Snowflake for data processing and analysis. From the initial data loading to advanced analysis in the Gold zone, the project encompasses all the necessary phases to transform raw data into valuable insights, thus facilitating informed and strategic decision-making.

## Contact
- Email: enriquehervasguerrero@gmail.com
- LinkedIn: https://www.linkedin.com/in/enrique-hervas-guerrero/
