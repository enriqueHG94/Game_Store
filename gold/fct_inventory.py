from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from credentials import sfOptionsSilver, sfOptionsGold

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("fct_inventory") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Defines the low stock threshold
low_stock_threshold = 10

# Reads from the silver base
fct_inventory = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "CSV.TRF_INVENTORY") \
    .load()

# Add a boolean column 'LOW_STOCK_ALERT' to indicate if the available stock is below the defined threshold
fct_inventory = fct_inventory.withColumn("LOW_STOCK_ALERT", col("available_stock") < low_stock_threshold)

# Reorder columns
fct_inventory = fct_inventory.select(
    "inventory_id",
    "store_id",
    "product_id",
    "available_stock",
    "LOW_STOCK_ALERT",  # Nueva columna justo después de available_stock
    "load_timestamp"
)

# Write on the gold base
fct_inventory.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsGold) \
    .option("dbtable", "csv.fct_inventory") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
