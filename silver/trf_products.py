from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, array, lit
from pyspark.sql.types import FloatType
from credentials import sfOptions, sfOptionsSilver

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("products_transformation") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads the products data from the bronze base
df_products = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CSV.PRODUCTS") \
    .load()

# Convert the column 'price' to FloatType
df_products = df_products.withColumn("price", col("price").cast(FloatType()))

# Convert the 'rating' column to FloatType
df_products = df_products.withColumn("rating", col("rating").cast(FloatType()))

# Ensures that all prices are positive
df_products = df_products.withColumn("price", when(col("price") < 0, -col("price")).otherwise(col("price")))

# Convert release_date to date type
df_products = df_products.withColumn("release_date", to_date(col("release_date"), "dd/MM/yyyy"))

# Ensures that all games have the 5 required languages
required_languages = ["English", "Chinese", "Hindi", "Spanish", "Arabic"]
df_products = df_products.withColumn("languages", array([lit(lang) for lang in required_languages]))

# Reorder the columns
df_products = df_products.select(
    "product_id",
    "provider_id",
    "product_name",
    "genre",
    "platform",
    "manufacturer",
    "languages",
    "recommended_age",
    "price",
    "rating",
    "weight",
    "dimensions",
    "release_date",
    "load_timestamp"
)

# Write the transformed data to the silver base
df_products.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "csv.trf_products") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
