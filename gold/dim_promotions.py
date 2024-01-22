from pyspark.sql import SparkSession
from credentials import sfOptionsSilver, sfOptionsGold

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("dim_promotions") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the silver base
dim_promotions = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "CSV.TRF_PROMOTIONS") \
    .load()

# Write on the gold base
dim_promotions.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsGold) \
    .option("dbtable", "csv.dim_promotions") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
