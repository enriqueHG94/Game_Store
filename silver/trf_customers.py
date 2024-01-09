from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace, current_date, year, when, length, lpad, expr
from pyspark.sql.types import IntegerType
from credentials import sfOptions, sfOptionsSilver

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("ELT_Process_Bronze_to_Silver") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the bronze base
df_bronze = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CSV.CUSTOMERS") \
    .load()

# Convert dates to date type
df_bronze = df_bronze.withColumn("BIRTH_DATE", to_date(col("birth_date"), "dd/MM/yyyy"))
df_bronze = df_bronze.withColumn("REGISTRATION_DATE", to_date(col("registration_date"), "dd/MM/yyyy"))

# Fill in postcodes with less than 5 digits with leading zeros
df_bronze = df_bronze.withColumn("POSTAL_CODE", lpad(col("POSTAL_CODE"), 5, '0'))

# Clear phone numbers to remove unwanted characters
df_bronze = df_bronze.withColumn("CLEAN_PHONE", regexp_replace(col("PHONE"), "[^\\d]", ""))

# Make sure all numbers are 9 digits long and add zeros if necessary.
df_bronze = df_bronze.withColumn("CLEAN_PHONE",
                                 when(length(col("CLEAN_PHONE")) == 9, col("CLEAN_PHONE"))
                                 .when(length(col("CLEAN_PHONE")) < 9, lpad(col("CLEAN_PHONE"), 9, '0'))
                                 .otherwise(regexp_replace(col("CLEAN_PHONE"), "^(34)", "")))

# Add the prefix +34 and reformat the telephone numbers.
df_bronze = df_bronze.withColumn("FORMATTED_PHONE",
                                 expr("concat('+34 ', substring(CLEAN_PHONE, 1, 3), ' ', substring(CLEAN_PHONE, 4, "
                                      "3), ' ', substring(CLEAN_PHONE, 7, 3))"))

# Convert loyalty_points to INT
df_bronze = df_bronze.withColumn("loyalty_points", col("loyalty_points").cast(IntegerType()))

# Calculate the age of clients
df_bronze = df_bronze.withColumn("AGE", year(current_date()) - year(col("birth_date")))

# Select and reorder columns
df_final = df_bronze.select(
    "CUSTOMER_ID",
    "CUSTOMER_NAME",
    "EMAIL",
    "FORMATTED_PHONE",
    "COUNTRY",
    "CITY",
    "ADDRESS",
    "POSTAL_CODE",
    "BIRTH_DATE",
    "AGE",
    "GENDER",
    "REGISTRATION_DATE",
    "PURCHASE_HISTORY",
    "LOYALTY_POINTS",
    "LOAD_TIMESTAMP"
)

# Rename the FORMATTED_PHONE column to PHONE
df_final = df_final.withColumnRenamed("FORMATTED_PHONE", "PHONE")

# Write on the silver base
df_final.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "csv.trf_customers") \
    .mode("append") \
    .save()
