import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}

// Initialize SparkSession
val spark = SparkSession.builder
  .appName("Divvy Trips 2015-Q1")
  .getOrCreate()

// Set up the path to the CSV file
val file_path = "file:///home/vagrant/dataset/Divvy_Trips_2015-Q1.csv"

// Method 1: Infer schema and read the CSV file
val df_inferred_schema = spark.read.option("header", true).option("inferSchema", true).csv(file_path)
println("Dataframe with inferred schema:")
df_inferred_schema.printSchema()
println("Count of records:", df_inferred_schema.count())

// Method 2: Programmatically use StructFields to create and attach a schema
val customSchema = StructType(
  StructField("trip_id", IntegerType, true) ::
  StructField("starttime", StringType, true) ::
  StructField("stoptime", StringType, true) ::
  StructField("bikeid", IntegerType, true) ::
  StructField("tripduration", DoubleType, true) ::
  StructField("from_station_id", IntegerType, true) ::
  StructField("from_station_name", StringType, true) ::
  StructField("to_station_id", IntegerType, true) ::
  StructField("to_station_name", StringType, true) ::
  StructField("usertype", StringType, true) ::
  StructField("gender", StringType, true) ::
  StructField("birthyear", IntegerType, true) :: Nil
)

val df_struct_fields_schema = spark.read.option("header", true).schema(customSchema).csv(file_path)
println("\nDataframe with StructFields schema:")
df_struct_fields_schema.printSchema()
println("Count of records:", df_struct_fields_schema.count())

// Method 3: Attach a schema via DDL
val ddlSchema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration DOUBLE, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

val df_ddl_schema = spark.read.option("header", true).schema(ddlSchema).csv(file_path)
println("\nDataframe with DDL schema:")
df_ddl_schema.printSchema()
println("Count of records:", df_ddl_schema.count())

// Stop SparkSession
spark.stop()