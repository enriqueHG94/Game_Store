from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, when, lit
from credentials import sfOptions, sfOptionsSilver

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("payment_methods_transformation") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads the payment methods data from the bronze base
df_payment_methods = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CSV.PAYMENT_METHODS") \
    .load()

# Standardize the processing fee: remove the percentage sign and convert the value to a decimal
df_payment_methods = df_payment_methods.withColumn(
    "processing_fee",
    regexp_replace(col("processing_fee"), "%", "").cast("float") / 100
)

# Convert the availability column to a boolean type where 'Sí' translates to true and 'No' to false
df_payment_methods = df_payment_methods.withColumn(
    "available",
    when(col("available") == "Sí", lit(True)).otherwise(lit(False))
)

# Write the transformed data to the silver base
df_payment_methods.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "csv.trf_payment_methods") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
