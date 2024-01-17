from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, regexp_replace, when
from credentials import sfOptions, sfOptionsSilver

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("shipments_transformation") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the bronze base
df_shipments = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CSV.SHIPMENTS") \
    .load()

# Convert SHIPPING_DATE and ESTIMATED_DELIVERY_DATE to standard time and date format
df_shipments = df_shipments.withColumn("SHIPPING_DATE", to_timestamp("SHIPPING_DATE", "dd/MM/yyyy H:mm"))
df_shipments = df_shipments.withColumn("ESTIMATED_DELIVERY_DATE",
                                       to_timestamp("ESTIMATED_DELIVERY_DATE", "dd/MM/yyyy H:mm"))

# Convert SHIPPING_COST to decimal and correct negative values
df_shipments = df_shipments.withColumn("SHIPPING_COST",
                                       regexp_replace(col("SHIPPING_COST"), ",",
                                                      ".").cast("decimal(10,2)"))
df_shipments = df_shipments.withColumn("SHIPPING_COST",
                                       when(col("SHIPPING_COST") < 0,-col("SHIPPING_COST")).
                                       otherwise(col("SHIPPING_COST")))

# Reorder columns
df_shipments = df_shipments.select(
    "SHIPPING_ID",
    "SALE_ID",
    "CUSTOMER_ID",
    "SHIPPING_ADDRESS",
    "SHIPPING_COMPANY",
    "DELIVERY_STATUS",
    "TRACKING_NUMBER",
    "SHIPPING_COST",
    "SHIPPING_DATE",
    "ESTIMATED_DELIVERY_DATE",
    "LOAD_TIMESTAMP"
)

# Write on the silver base
df_shipments.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "csv.trf_shipments") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
