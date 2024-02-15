import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FireCallsAnalysis {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Fire Calls Analysis")
      .getOrCreate()

    // Define schema for the dataset
    val schema = StructType(Array(
      StructField("CallNumber", IntegerType, nullable = true),
      StructField("UnitID", StringType, nullable = true),
      StructField("IncidentNumber", IntegerType, nullable = true),
      StructField("CallType", StringType, nullable = true),
      StructField("CallDate", StringType, nullable = true),
      StructField("WatchDate", StringType, nullable = true),
      StructField("CallFinalDisposition", StringType, nullable = true),
      StructField("AvailableDtTm", StringType, nullable = true),
      StructField("Address", StringType, nullable = true),
      StructField("City", StringType, nullable = true),
      StructField("Zipcode", IntegerType, nullable = true),
      StructField("Battalion", StringType, nullable = true),
      StructField("StationArea", StringType, nullable = true),
      StructField("Box", StringType, nullable = true),
      StructField("OriginalPriority", StringType, nullable = true),
      StructField("Priority", StringType, nullable = true),
      StructField("FinalPriority", StringType, nullable = true),
      StructField("ALSUnit", BooleanType, nullable = true),
      StructField("CallTypeGroup", StringType, nullable = true),
      StructField("NumAlarms", IntegerType, nullable = true),
      StructField("UnitType", StringType, nullable = true),
      StructField("UnitSequenceInCallDispatch", IntegerType, nullable = true),
      StructField("FirePreventionDistrict", StringType, nullable = true),
      StructField("SupervisorDistrict", StringType, nullable = true),
      StructField("Neighborhood", StringType, nullable = true),
      StructField("Location", StringType, nullable = true),
      StructField("RowID", StringType, nullable = true),
      StructField("Delay", FloatType, nullable = true)
    ))

    // Read the CSV file
    val df = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load("sf-fire-calls.csv")

    // Register DataFrame as a temporary view
    df.createOrReplaceTempView("fire_calls")

    // Question 1: What were all the different types of fire calls in 2018?
    val fireCallTypes2018 = spark.sql("SELECT DISTINCT CallType FROM fire_calls WHERE year(to_date(CallDate, 'MM/dd/yyyy')) = 2018")
    fireCallTypes2018.show()

    // Question 2: What months within the year 2018 saw the highest number of fire calls?
    val monthlyFireCalls2018 = spark.sql("SELECT month(to_date(CallDate, 'MM/dd/yyyy')) AS Month, COUNT(*) AS NumCalls FROM fire_calls WHERE year(to_date(CallDate, 'MM/dd/yyyy')) = 2018 GROUP BY Month ORDER BY NumCalls DESC")
    monthlyFireCalls2018.show()

    // Question 3: Which neighborhood in San Francisco generated the most fire calls in 2018?
    val neighborhoodMostFireCalls2018 = spark.sql("SELECT Neighborhood, COUNT(*) AS NumCalls FROM fire_calls WHERE year(to_date(CallDate, 'MM/dd/yyyy')) = 2018 GROUP BY Neighborhood ORDER BY NumCalls DESC LIMIT 1")
    neighborhoodMostFireCalls2018.show()

    // Question 4: Which neighborhoods had the worst response times to fire calls in 2018?
    // We can't answer this question with the provided data.

    // Question 5: Which week in the year in 2018 had the most fire calls?
    val weeklyFireCalls2018 = spark.sql("SELECT weekofyear(to_date(CallDate, 'MM/dd/yyyy')) AS Week, COUNT(*) AS NumCalls FROM fire_calls WHERE year(to_date(CallDate, 'MM/dd/yyyy')) = 2018 GROUP BY Week ORDER BY NumCalls DESC LIMIT 1")
    weeklyFireCalls2018.show()

    // Question 6: Is there a correlation between neighborhood, zip code, and number of fire calls?
    // We can analyze this using correlation functions or statistical tests. 

    // Question 7: How can we use Parquet files or SQL tables to store this data and read it back?
    // We can save the DataFrame as Parquet files or register it as a temporary SQL table.
    df.write.parquet("sf_fire_calls_2018.parquet")

    // Stop SparkSession
    spark.stop()
  }
}