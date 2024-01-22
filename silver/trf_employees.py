from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, when, col, regexp_replace, lit, format_string, concat, length, lpad, expr, \
    to_date, unix_timestamp
from pyspark.sql.types import IntegerType
from credentials import sfOptions, sfOptionsSilver

# Initialize a Spark session with the necessary Snowflake JARs
spark = SparkSession.builder.appName("employees_transformation") \
    .config("spark.jars", "../jars/snowflake-jdbc-3.14.4.jar,../jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar") \
    .getOrCreate()

# Reads from the bronze base
df_employees = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CSV.EMPLOYEES") \
    .load()


# Function to generate the email based on name and store_id
def generate_email(name_col, store_id_col):
    name_parts = regexp_replace(lower(name_col), "\\s+", ".")
    store_id_formatted = format_string("%03d", store_id_col.cast("int"))
    return concat(name_parts, lit("@gamezone"), store_id_formatted, lit(".com"))


# Applies the function to generate the emails where the email is missing
df_employees = df_employees.withColumn(
    "email",
    when(
        col("email").isNull(),
        generate_email(col("employee_name"), col("store_id"))
    ).otherwise(col("email"))
)

# Adds prefix +34 if not present and removes non-numeric characters
df_employees = df_employees.withColumn("phone",
                                       when(col("phone").isNull(), "+34 000 000 000")
                                       .otherwise(
                                           when(col("phone").startswith("+34"), col("phone"))
                                           .otherwise(concat(lit("+34 "), col("phone")))
                                       ))

# Removes unwanted characters and additional spaces, if any
df_employees = df_employees.withColumn("phone", regexp_replace(col("phone"), "[^+\\d]", ""))

# Ensure that all numbers are correctly formatted
df_employees = df_employees.withColumn("phone",
                                       expr("CASE WHEN phone LIKE '+34%' THEN phone " +
                                            "WHEN LENGTH(regexp_replace(phone, '[^0-9]', '')) = 9 THEN concat('+34 ', "
                                            "phone) " +
                                            "ELSE '+34 000 000 000' END"))

# Format the phone number so that it has spaces in the correct positions.
df_employees = df_employees.withColumn("phone",
                                       expr("concat(substring(phone, 1, 3), ' ', " +
                                            "substring(phone, 4, 3), ' ', " +
                                            "substring(phone, 7, 3), ' ', " +
                                            "substring(phone, 10, 3))"))

# Convert salary and weekly_hours to Integer type
df_employees = df_employees.withColumn("SALARY", col("SALARY").cast(IntegerType()))
df_employees = df_employees.withColumn("WEEKLY_HOURS", col("WEEKLY_HOURS").cast(IntegerType()))

# Convert dates to Date type, also considering the time part
date_format = "dd/MM/yyyy H:mm"
df_employees = df_employees.withColumn("BIRTH_DATE", to_date(unix_timestamp(col("BIRTH_DATE"),
                                                                            date_format).cast("timestamp")))
df_employees = df_employees.withColumn("HIRE_DATE", to_date(unix_timestamp(col("HIRE_DATE"),
                                                                           date_format).cast("timestamp")))

# Clear social security number and ensure correct formatting
df_employees = df_employees.withColumn("formatted_ssn",
                                       when(col("social_security_number").isNull(), lit("000-00-0000"))
                                       .otherwise(
                                           concat(
                                               lpad(regexp_replace(col("social_security_number"), "\\D", ""),
                                                    9, '0').substr(1, 3),
                                               lit('-'),
                                               lpad(regexp_replace(col("social_security_number"), "\\D", ""),
                                                    9, '0').substr(4, 2),
                                               lit('-'),
                                               lpad(regexp_replace(col("social_security_number"), "\\D", ""),
                                                    9, '0').substr(6, 4)
                                           )
                                       )
                                       )

# Reorder the columns
df_employees = df_employees.select(
    "employee_id",
    "store_id",
    "employee_name",
    "role",
    "email",
    "phone",
    "salary",
    "weekly_hours",
    "nationality",
    "region",
    "city",
    "employee_address",
    "postal_code",
    "formatted_ssn",
    "birth_date",
    "hire_date",
    "load_timestamp"
)

# Rename column 'formatted_ssn' to 'social_security_number'
df_employees = df_employees.withColumnRenamed("formatted_ssn", "social_security_number")

# Remove duplicates in the column 'employee_id'
df_employees = df_employees.dropDuplicates(["employee_id"])

# Write on the silver base
df_employees.write.format("net.snowflake.spark.snowflake") \
    .options(**sfOptionsSilver) \
    .option("dbtable", "csv.trf_employees") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
