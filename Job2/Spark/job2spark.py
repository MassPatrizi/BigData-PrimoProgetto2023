from pyspark.sql import SparkSession


# Crea la sessione Spark
spark = SparkSession.builder.appName("ReviewsAnalysisJob2").getOrCreate()

# Carica i dati
reviews = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("file:///Users/alessandro/Development/BigData/BigData-PrimoProgetto2023/ReviewsCleaned.csv")

# Generazione dei risultati



# qua la parte di processing


# Salva i risultati in un file locale
final_results.write.csv("file:///Users/Massimiliano/Desktop/final_results.csv", header=True, mode="overwrite")

# Chiudi la sessione Spark
spark.stop()
