from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from credentials import sfOptions

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("CSVToSnowflake") \
    .config("spark.jars", "jars/snowflake-jdbc-3.14.4.jar,jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()


def read_csv_file(file_path: str) -> DataFrame:
    """
    Reads a CSV file into a Spark DataFrame.

    Parameters:
    - file_path (str): The path to the CSV file to read.

    Returns:
    - DataFrame: A Spark DataFrame containing the CSV data.
    """
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", ";") \
        .option("encoding", "UTF-8") \
        .load(file_path)


def write_to_snowflake(df: DataFrame, table_name: str):
    """
    Writes a DataFrame to a Snowflake table, appending a load timestamp.

    Parameters:
    - df (DataFrame): The DataFrame to write to Snowflake.
    - table_name (str): The name of the target Snowflake table.
    """
    df_with_timestamp = df.withColumn("load_timestamp", current_timestamp())
    df_with_timestamp.write.format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", table_name) \
        .option("charset", "UTF-8") \
        .mode("append") \
        .save()


# A mapping of CSV file paths to Snowflake table names
csv_files_to_tables = {
    "bronze/customers.csv": "csv.customers",
    "bronze/employees.csv": "csv.employees",
    "bronze/inventory.csv": "csv.inventory",
    "bronze/payment_methods.csv": "csv.payment_methods",
    "bronze/products.csv": "csv.products",
    "bronze/promotions.csv": "csv.promotions",
    "bronze/providers.csv": "csv.providers",
    "bronze/sales.csv": "csv.sales",
    "bronze/shipments.csv": "csv.shipments",
    "bronze/stores.csv": "csv.stores"
}

# Process and load each CSV file into its corresponding Snowflake table
for csv_file, snowflake_table in csv_files_to_tables.items():
    try:
        # Read the CSV file into a DataFrame
        df = read_csv_file(csv_file)
        # Write the DataFrame to Snowflake
        write_to_snowflake(df, snowflake_table)
    except Exception as e:
        # Log the exception and continue with the next file
        print(f"An error occurred while processing {csv_file}: {e}")

# Stop the Spark session
spark.stop()
