from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, regexp_replace, when
from credentials import sfOptions, sfOptionsSilver

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("sales_transformation") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the bronze base
df_sales = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CSV.SALES") \
    .load()

# Converts SALE_DATE to a standard date and time format
df_sales = df_sales.withColumn("SALE_DATE", to_timestamp("SALE_DATE", "dd/MM/yyyy H:mm"))

# Converts UNIT_PRICE and TOTAL_PRICE to decimals and corrects for negative values
df_sales = df_sales.withColumn("UNIT_PRICE", regexp_replace(col("UNIT_PRICE"),
                                                            ",", ".").cast("decimal(10,2)"))
df_sales = df_sales.withColumn("TOTAL_PRICE", regexp_replace(col("TOTAL_PRICE"),
                                                             ",", ".").cast("decimal(10,2)"))
df_sales = df_sales.withColumn("UNIT_PRICE", when(col("UNIT_PRICE") < 0,
                                                  -col("UNIT_PRICE")).otherwise(col("UNIT_PRICE")))
df_sales = df_sales.withColumn("TOTAL_PRICE", when(col("TOTAL_PRICE") < 0,
                                                   -col("TOTAL_PRICE")).otherwise(col("TOTAL_PRICE")))

# Replace null values in PROMOTION_ID with PROMO401.
df_sales = df_sales.fillna({"PROMOTION_ID": "PROMO401"})

# Reorder the columns
df_sales = df_sales.select(
    "SALE_ID",
    "CUSTOMER_ID",
    "EMPLOYEE_ID",
    "STORE_ID",
    "PRODUCT_ID",
    "PAYMENT_METHOD_ID",
    "PROMOTION_ID",
    "COMMENTS",
    "QUANTITY",
    "UNIT_PRICE",
    "TOTAL_PRICE",
    "SALE_DATE",
    "LOAD_TIMESTAMP"
)


# Write on the silver base
df_sales.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "csv.trf_sales") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
