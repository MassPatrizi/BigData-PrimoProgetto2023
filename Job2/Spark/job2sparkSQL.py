from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, regexp_replace, length, split, explode, from_unixtime, row_number, sum as Ssum, desc as Sdesc
from pyspark.sql.window import Window

# Crea la sessione Spark
spark = SparkSession.builder.appName("ReviewsAnalysis").getOrCreate()

# Carica i dati
reviews = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("file:///Users/alessandro/Development/BigData/BigData-PrimoProgetto2023/ReviewsCleaned.csv")

userId_Num_Den = reviews.select(
    col("UserId").alias("userid"),
    col("HelpfulnessNumerator").cast("float").alias("helpfulnessnumerator"),
    col("HelpfulnessDenominator").cast("float").alias("helpfulnessdenominator")
)

userId_appreciation = userId_Num_Den \
  .groupBy("userid") \
  .agg(Ssum(col("helpfulnessnumerator")).alias("sum_numerator"), Ssum(col("helpfulnessdenominator")).alias("sum_denominator")) \
  .withColumn("mean", col("sum_numerator")/col("sum_denominator")) \
  .orderBy(Sdesc(col("mean"))) \
  .limit(10) \
  .select("userid", "mean")

userId_appreciation.show()