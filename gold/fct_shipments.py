from pyspark.sql import SparkSession
from credentials import sfOptionsSilver, sfOptionsGold

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("fct_shipments") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the silver base
fct_shipments = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "CSV.TRF_SHIPMENTS") \
    .load()

# Write on the gold base
fct_shipments.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsGold) \
    .option("dbtable", "csv.fct_shipments") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
