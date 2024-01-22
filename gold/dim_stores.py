from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from credentials import sfOptionsSilver, sfOptionsGold

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("dim_stores") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the silver base
dim_stores = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "CSV.TRF_STORES") \
    .load()

# Add the column 'country' with all values as 'España'.
dim_stores = dim_stores.withColumn("COUNTRY", lit("España"))

# Reorder columns
dim_stores = dim_stores.select(
    "STORE_ID",
    "STORE_NAME",
    "PHONE",
    "EMAIL_CONTACT",
    "NUMBER_EMPLOYEES",
    "COUNTRY",
    "REGION",
    "CITY",
    "ADDRESS",
    "POSTAL_CODE",
    "OPENING_DATE",
    "SCHEDULE",
    "LOAD_TIMESTAMP"
)

# Write on the gold base
dim_stores.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsGold) \
    .option("dbtable", "csv.dim_stores") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
