from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from credentials import sfOptionsSilver, sfOptionsGold

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("dim_employees") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the silver base
dim_employees = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "CSV.TRF_EMPLOYEES") \
    .load()

# Add column 'COUNTRY' with all values as 'España'
dim_employees = dim_employees.withColumn("COUNTRY", lit("España"))

# Reorder columns
dim_employees = dim_employees.select(
    "EMPLOYEE_ID",
    "STORE_ID",
    "EMPLOYEE_NAME",
    "ROLE",
    "EMAIL",
    "PHONE",
    "SALARY",
    "WEEKLY_HOURS",
    "NATIONALITY",
    "COUNTRY",
    "CITY",
    "EMPLOYEE_ADDRESS",
    "POSTAL_CODE",
    "BIRTH_DATE",
    "HIRE_DATE",
    "LOAD_TIMESTAMP"
)

# Write on the gold base
dim_employees.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsGold) \
    .option("dbtable", "csv.dim_employees") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
