from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, regexp_replace, length, split, explode, from_unixtime, row_number
from pyspark.sql.window import Window

# Crea la sessione Spark
spark = SparkSession.builder.appName("ReviewsAnalysis").getOrCreate()

# Carica i dati
reviews = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("file:///Users/Massimiliano/Desktop/ReviewsCleaned.csv")

# Generazione dei risultati per ciascun anno
reviews_by_year = reviews.select(
    year(from_unixtime(col("Time"), "yyyy-MM-dd HH:mm:ss")).alias("year"),
    col("ProductId").alias("productid"),
    col("Text").alias("text")
)

top_products = reviews_by_year.groupBy("year", "productid").count().withColumnRenamed("count", "review_count")

window_spec = Window.partitionBy("year").orderBy(col("review_count").desc())
top_10_products = top_products.withColumn("rank", row_number().over(window_spec)).where(col("rank") <= 10)

word_counts = reviews_by_year.select(
    "year",
    "productid",
    regexp_replace(col("text"), "[^a-zA-Z0-9\s]", "").alias("cleaned_text")
).withColumn("word", explode(split(col("cleaned_text"), " "))).where(length(col("word")) >= 4).groupBy(
    "year",
    "productid",
    "word"
).count().withColumnRenamed("count", "word_count")

window_spec_word = Window.partitionBy("year", "productid").orderBy(col("word_count").desc())
top_words = word_counts.withColumn("rank", row_number().over(window_spec_word)).where(col("rank") <= 5)

final_results = top_10_products.join(top_words, ["year", "productid"]).orderBy("year", col("review_count").desc(), "productid", col("word_count").desc()).drop("rank")

# Stampa i risultati (prime 20 righe)
final_results.show()

# Salva i risultati in un file locale
final_results.write.csv("file:///Users/Massimiliano/Desktop/final_results.csv", header=True, mode="overwrite")

# Chiudi la sessione Spark
spark.stop()
