from pyspark.sql import SparkSession
from credentials import sfOptionsSilver, sfOptionsGold

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("dim_providers") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the silver base
dim_providers = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "CSV.TRF_PROVIDERS") \
    .load()

# Reorder columns
dim_providers = dim_providers.select(
    "PROVIDER_ID",
    "PROVIDER_NAME",
    "CONTACT_NAME",
    "PHONE",
    "EMAIL_CONTACT",
    "PRODUCT_TYPE",
    "WEBSITE",
    "COUNTRY_OF_ORIGIN",
    "CITY",
    "PROVIDER_ADDRESS",
    "POSTAL_CODE",
    "PAYMENT_TERMS",
    "START_DATE",
    "LOAD_TIMESTAMP"
)

# Write on the gold base
dim_providers.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsGold) \
    .option("dbtable", "csv.dim_providers") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
