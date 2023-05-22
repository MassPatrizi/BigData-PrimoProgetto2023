import argparse
import datetime
from operator import add
from pyspark.sql import SparkSession
import re
import time

# Tempo all'inizio dell'esecuzione
start_time = time.time()


# Scommentare il dataset che si vuole utilizzare
input_filepath = "file:///Users/Massimiliano/Desktop/Datasets/ReviewsCleaned.csv"
#input_filepath = "file:///Users/Massimiliano/Desktop/duplicated_output.csv"
#input_filepath = "file:///Users/Massimiliano/Desktop/quintupled_output.csv"
#input_filepath = "file:///Users/Massimiliano/Desktop/decupled_output.csv"

# Inizializza la SparkSession (la memoria va aumentata al crescere del dataset)
spark = SparkSession \
    .builder \
    .appName("Job 1") \
    .config("spark.executor.memory", "800m") \
    .getOrCreate()

input_rdd = spark.sparkContext.textFile(input_filepath)

# Salta l'header del file
header = input_rdd.first()
data_rdd = input_rdd.filter(lambda x: x != header)

data_rdd.flatMap(lambda x: x.split("\n"))
parsed_rdd = data_rdd.map(lambda x: x.split(";", 9)) \
    .map(lambda x: (x[1], str(datetime.datetime.utcfromtimestamp(int(x[7])).year), x[9].lower()))

# Conta le recensioni e ne concatena i testi se appartenenti allo stesso anno
grouped_rdd = parsed_rdd.map(lambda x: ((x[1], x[0]), (1, x[2]))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + ' ' + y[1])) \
    .map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1]))

# Seleziona i 10 prodotti con più recensioni per ogni anno
top_10_rdd = grouped_rdd.groupBy(lambda x: x[0]) \
    .flatMap(lambda x: sorted(x[1], key=lambda y: y[2], reverse=True)[:10]) \
    .sortBy(lambda x: x[0], ascending=False)

# Divisione del campo "Text" in parole
split_text_rdd = top_10_rdd.map(lambda x: (x[0], x[1], x[2], x[3].split(" ")))

# Conteggio delle parole di lunghezza >=4 per ogni prodotto di ogni anno
word_counts = split_text_rdd.flatMap(lambda x: [((x[0], x[1], x[2]), word) for word in x[3]]) \
    .filter(lambda word: re.match(r'^[a-zA-Z]{4,}$', word[1])) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(add) \
    .map(lambda x: (x[0][0][0], x[0][0][1], x[0][0][2], (x[0][1], x[1]))) \
    .groupBy(lambda x: (x[1], x[0])) \
    .flatMap(lambda x: sorted(x[1], key=lambda y: y[3][1], reverse=True)[:5]) \
    .sortBy(lambda x: (x[0], x[2]), ascending=False)


# Creazione del DataFrame e stampa
df = word_counts.toDF(["year", "productid", "reviews count", "(word, count)"])
df.show(100)

# Tempo al termine dell'esecuzione
finish_time = time.time()

# Stampa il tempo impiegato
print(f"\n\nExecution Time: {finish_time - start_time}\n\n")

