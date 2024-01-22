from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from credentials import sfOptionsGold

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("dim_time") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Create a DataFrame with a range of dates
start_date = "1953-01-01"
end_date = "2030-12-31"

# Creates a date range where each date becomes a separate row in the DataFrame,
# with a column called DATE containing these dates.
df_time = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as DATE")

# Decompose the date
df_time = df_time.withColumn("YEAR", F.year("DATE")) \
    .withColumn("QUARTER", F.quarter("DATE")) \
    .withColumn("MONTH", F.month("DATE")) \
    .withColumn("MONTH_NAME", F.date_format("DATE", "MMMM")) \
    .withColumn("DAY", F.dayofmonth("DATE")) \
    .withColumn("DAY_NAME", F.date_format("DATE", "EEEE")) \
    .withColumn("DAY_OF_WEEK", F.dayofweek("DATE")) \
    .withColumn("DAY_OF_YEAR", F.dayofyear("DATE")) \
    .withColumn("WEEK_OF_YEAR", F.weekofyear("DATE")) \
    .withColumn("WEEK_OF_MONTH", (F.dayofmonth("DATE") - 1) / 7 + 1) \
    .withColumn("SEMESTER", F.when(F.col("MONTH") <= 6, 1).otherwise(2)) \
    .withColumn("IS_WEEKEND", F.when(F.col("DAY_OF_WEEK").isin(1, 7), F.lit(True)).otherwise(F.lit(False))) \
    .withColumn("IS_LEAP_YEAR", F.when(F.year("DATE") % 4 == 0, True).otherwise(False)) \
    .withColumn("TIME_ID", F.monotonically_increasing_id())

# Reorder columns
df_time = df_time.select(
    "TIME_ID",
    "DATE",
    "YEAR",
    "QUARTER",
    "SEMESTER",
    "MONTH",
    "MONTH_NAME",
    "WEEK_OF_YEAR",
    "WEEK_OF_MONTH",
    "DAY",
    "DAY_NAME",
    "DAY_OF_WEEK",
    "DAY_OF_YEAR",
    "IS_WEEKEND",
    "IS_LEAP_YEAR"
)
# Write on the gold base
df_time.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsGold) \
    .option("dbtable", "csv.dim_time") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
