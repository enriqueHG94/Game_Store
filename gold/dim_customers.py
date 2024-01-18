from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from credentials import sfOptionsSilver, sfOptionsGold

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("dim_customers") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the silver base
dim_customers = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "CSV.TRF_CUSTOMERS") \
    .load()

# Categorizing age
dim_customers = dim_customers.withColumn('AGE_GROUP',
                                         when(col('AGE') < 30, 'Young')
                                         .when(col('AGE') < 60, 'Adult')
                                         .otherwise('Senior'))

# Add the new column REGION
dim_customers = dim_customers.withColumn('REGION', col('CITY'))

# Reorder columns
dim_customers = dim_customers.select('CUSTOMER_ID', 'CUSTOMER_NAME', 'EMAIL', 'PHONE',
                                     'COUNTRY', 'REGION', 'CITY', 'ADDRESS',
                                     'POSTAL_CODE', 'BIRTH_DATE', 'AGE', 'AGE_GROUP',
                                     'GENDER', 'REGISTRATION_DATE', 'PURCHASE_HISTORY',
                                     'LOYALTY_POINTS', 'LOAD_TIMESTAMP')

# Write on the gold base
dim_customers.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsGold) \
    .option("dbtable", "csv.dim_customers") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
