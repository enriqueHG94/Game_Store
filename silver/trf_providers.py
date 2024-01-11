from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, to_date, expr, translate
from pyspark.sql.types import IntegerType
from credentials import sfOptions, sfOptionsSilver

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("providers_transformation") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads the providers data from the bronze base
df_providers = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CSV.PROVIDERS") \
    .load()

# Standardise telephone numbers to the format +34 nnn nnn nnn nnn
df_providers = df_providers.withColumn("phone",
                                       translate(regexp_replace("phone", "[^0-9]",
                                                                ""), " ", ""))
df_providers = df_providers.withColumn("phone",
                                       regexp_replace("phone", "(\\d{2})(\\d{3})(\\d{3})(\\d{3})",
                                                      "+34 $2 $3 $4"))


# Convert 'start_date' to Date type
df_providers = df_providers.withColumn(
    "start_date",
    to_date(col("start_date"), "dd/MM/yyyy")
)

# Transform 'payment_terms' to a numeric representation of credit days
df_providers = df_providers.withColumn(
    "payment_terms",
    regexp_replace(col("payment_terms"), "\\D+", "").cast(IntegerType())
)

# Reorder the columns
df_providers = df_providers.select(
    "provider_id",
    "provider_name",
    "contact_name",
    "email_contact",
    "phone",
    "provider_address",
    "postal_code",
    "city",
    "country_of_origin",
    "product_type",
    "payment_terms",
    "website",
    "start_date",
    "load_timestamp"
)

# Write the transformed data to the silver base
df_providers.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "csv.trf_providers") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()

