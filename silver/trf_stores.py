from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace
from pyspark.sql.types import IntegerType

from credentials import sfOptions, sfOptionsSilver

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("stores_transformation") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the bronze base
df_stores = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CSV.STORES") \
    .load()

# Convert opening_date to Date type
df_stores = df_stores.withColumn("opening_date", to_date(col("opening_date"), "dd/MM/yyyy"))

# Validates and corrects emails
df_stores = df_stores.withColumn("email_contact", regexp_replace(col("email_contact"), " ", ""))

# Convert number_employees to Integer type
df_stores = df_stores.withColumn("number_employees", col("number_employees").cast(IntegerType()))

# Write on the silver base
df_stores.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "csv.trf_stores") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
