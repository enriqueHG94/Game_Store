from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from credentials import sfOptions, sfOptionsSilver

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("Inventory_transformation") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the bronze base
df_inventory = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CSV.INVENTORY") \
    .load()

# Rename columns
df_inventory = df_inventory.withColumnRenamed('INVENTARIO_ID', 'TEMP1') \
    .withColumnRenamed('PRODUCT_ID', 'TEMP2') \
    .withColumnRenamed('STORE_ID', 'TEMP3') \
    .withColumnRenamed('AVAILABLE_STOCK', 'INVENTARIO_ID') \
    .withColumnRenamed('TEMP1', 'PRODUCT_ID') \
    .withColumnRenamed('TEMP2', 'STORE_ID') \
    .withColumnRenamed('TEMP3', 'AVAILABLE_STOCK') \
    .withColumnRenamed('INVENTARIO_ID', 'INVENTORY_ID')

# Change negative values to 0 and change type to integer
df_inventory = df_inventory.withColumn('AVAILABLE_STOCK',
                                       when(col('AVAILABLE_STOCK') < 0, 0)
                                       .otherwise(col('AVAILABLE_STOCK').cast('integer')))

# Remove duplicates based on 'INVENTORY_ID'.
df_inventory = df_inventory.dropDuplicates(['INVENTORY_ID'])

# Reorder columns
df_inventory = df_inventory.select('INVENTORY_ID', 'STORE_ID', 'PRODUCT_ID', 'AVAILABLE_STOCK', 'LOAD_TIMESTAMP')

# Write on the silver base
df_inventory.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "csv.trf_inventory") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
