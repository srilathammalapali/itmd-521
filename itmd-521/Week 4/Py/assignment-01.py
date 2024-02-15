from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Divvy Trips 2015-Q1") \
    .getOrCreate()

# Set up the path to the CSV file
file_path = "file:///home/vagrant/dataset/Divvy_Trips_2015-Q1.csv"

# Method 1: Infer schema and read the CSV file
df_inferred_schema = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
print("Dataframe with inferred schema:")
df_inferred_schema.printSchema()
print("Count of records:", df_inferred_schema.count())

# Method 2: Programmatically use StructFields to create and attach a schema
custom_schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("starttime", StringType(), True),
    StructField("stoptime", StringType(), True),
    StructField("bikeid", IntegerType(), True),
    StructField("tripduration", DoubleType(), True),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True)
])

df_struct_fields_schema = spark.read.option("header", True).schema(custom_schema).csv(file_path)
print("\nDataframe with StructFields schema:")
df_struct_fields_schema.printSchema()
print("Count of records:", df_struct_fields_schema.count())

# Method 3: Attach a schema via DDL
ddl_schema = """
    trip_id INT,
    starttime STRING,
    stoptime STRING,
    bikeid INT,
    tripduration DOUBLE,
    from_station_id INT,
    from_station_name STRING,
    to_station_id INT,
    to_station_name STRING,
    usertype STRING,
    gender STRING,
    birthyear INT
"""

df_ddl_schema = spark.read.option("header", True).schema(ddl_schema).csv(file_path)
print("\nDataframe with DDL schema:")
df_ddl_schema.printSchema()
print("Count of records:", df_ddl_schema.count())


spark = SparkSession.builder \
    .appName("Divvy Trips 2015-Q1") \
    .getOrCreate()

# Set up the path to the CSV file
file_path = "file:///home/vagrant/dataset/Divvy_Trips_2015-Q1.csv"

# Read the CSV file
df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)

# Transformation: Select Gender based on last name
df_filtered = df.select("from_station_name", "gender").filter(df["gender"].isin(["Female", "Male"]))

# Transformation: GroupBy station name
df_grouped = df_filtered.groupBy("from_station_name", "gender").count()

# Action: Show 10 records of the DataFrame
df_grouped.show(10)


# Stop SparkSession
spark.stop()