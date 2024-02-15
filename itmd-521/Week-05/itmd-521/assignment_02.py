from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, weekofyear, count, col

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Fire Calls Analysis") \
    .getOrCreate()

# Load the dataset into a DataFrame
df = spark.read.csv("sf-fire-calls.csv", header=True, inferSchema=True)

# Filter data for the year 2018
df_2018 = df.filter(year("CallDate") == 2018)

# Question 1: What were all the different types of fire calls in 2018?
fire_call_types_2018 = df_2018.select("CallType").distinct().collect()
print("Different types of fire calls in 2018:")
for row in fire_call_types_2018:
    print(row[0])

# Question 2: What months within the year 2018 saw the highest number of fire calls?
monthly_fire_calls_2018 = df_2018.groupBy(month("CallDate").alias("Month")).count().orderBy("count", ascending=False).collect()
print("Months within the year 2018 with the highest number of fire calls:")
for row in monthly_fire_calls_2018:
    print("Month:", row["Month"], "| Number of Fire Calls:", row["count"])

# Question 3: Which neighborhood in San Francisco generated the most fire calls in 2018?
neighborhood_fire_calls_2018 = df_2018.groupBy("Neighborhood").count().orderBy("count", ascending=False).first()
print("Neighborhood in San Francisco with the most fire calls in 2018:", neighborhood_fire_calls_2018["Neighborhood"])

# Question 4: Which neighborhoods had the worst response times to fire calls in 2018?
# We can't answer this question with the provided data.

# Question 5: Which week in the year in 2018 had the most fire calls?
weekly_fire_calls_2018 = df_2018.groupBy(weekofyear("CallDate").alias("Week")).count().orderBy("count", ascending=False).collect()
most_fire_calls_week_2018 = weekly_fire_calls_2018[0]["Week"]
print("Week in the year 2018 with the most fire calls:", most_fire_calls_week_2018)

# Question 6: Is there a correlation between neighborhood, zip code, and number of fire calls?
# We can analyze this using correlation functions or statistical tests.

# Question 7: How can we use Parquet files or SQL tables to store this data and read it back?
# We can save the DataFrame as Parquet files or register it as a temporary SQL table.

# Save DataFrame as Parquet files
df_2018.write.parquet("sf_fire_calls_2018.parquet")

# Register DataFrame as a temporary SQL table
df_2018.createOrReplaceTempView("fire_calls_2018")

# Read back Parquet files
df_parquet = spark.read.parquet("sf_fire_calls_2018.parquet")

# Query SQL table
spark.sql("SELECT * FROM fire_calls_2018").show()

# Stop SparkSession
spark.stop()