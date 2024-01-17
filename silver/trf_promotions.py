from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, regexp_replace, abs, unix_timestamp
from pyspark.sql.types import FloatType
from credentials import sfOptions, sfOptionsSilver

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("promotions_transformation") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the bronze base
df_promotions = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CSV.PROMOTIONS") \
    .load()

# Convert dates to Date type, also considering the time part
df_promotions = df_promotions.withColumn("START_DATE", to_date(unix_timestamp(col("START_DATE"),
                                                                              'dd/MM/yyyy H:mm').cast("timestamp")))
df_promotions = df_promotions.withColumn("END_DATE", to_date(unix_timestamp(col("END_DATE"),
                                                                              'dd/MM/yyyy H:mm').cast("timestamp")))

# Normalize discount values and ensure they are positive
df_promotions = df_promotions.withColumn("DISCOUNT_VALUE",
                                         regexp_replace(col("discount_value"), "%",
                                                        "").cast(FloatType()))
df_promotions = df_promotions.withColumn("DISCOUNT_VALUE", abs(col("DISCOUNT_VALUE")))

# Remove duplicate promotions based on 'promotion_id'
df_promotions = df_promotions.dropDuplicates(["promotion_id"])

# Reorder the columns
df_promotions = df_promotions.select(
    "promotion_id",
    "promotion_name",
    "discount_type",
    "terms_and_conditions",
    "discount_value",
    "start_date",
    "end_date",
    "load_timestamp"
)

# Write on the silver base
df_promotions.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "csv.trf_promotions") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
