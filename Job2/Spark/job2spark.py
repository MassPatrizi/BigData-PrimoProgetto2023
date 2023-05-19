#!/usr/bin/env python3
"""spark application"""
import argparse
from pyspark.sql import SparkSession

def float_division(a, b):
    if(b!=0):
        result = a/b
    else:
        result = 0
    return result

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# create the Spark session
spark = SparkSession.builder.appName("ReviewsAnalysisJob2").getOrCreate()

# use the Spark session to create RDD from the input data
input_RDD = spark.sparkContext.textFile(input_filepath).cache()

# remove header
header = input_RDD.first()
input_RDD = input_RDD.filter(f=lambda line: line != header)

# variable used in aggregationByKey phase
aTuple = (0,0)

# compute the average of utilities:
# (1) create a RDD splitting the csv by ";"
# (2) map into userID, HelpNum, HelpDenom
# (3) map into userID, utility
# (4) aggregateByKey, summing utilities and counting occurrencies
# (5) compute the mean
# (6) sort the RDD
# (7) save as text file
sorted_utility_average_RDD = input_RDD.map(f=lambda line: line.strip().split(";")) \
                .map(f=lambda line: (line[2], line[4], line[5])) \
                .map(f=lambda line: (line[0], float_division(float(line[1]),float(line[2])))) \
                .aggregateByKey(aTuple, lambda a,b: (a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1])) \
                .mapValues(lambda v: v[0]/v[1]) \
                .sortBy(lambda x: -x[1]) \
                .saveAsTextFile(output_filepath)

# explanation of aggregateByKey line is here
# https://stackoverflow.com/questions/29930110/calculating-the-averages-for-each-key-in-a-pairwise-k-v-rdd-in-spark-with-pyth
# first lambda is aggregation within-partition, second lambda is cross-partition

# close Spark session
spark.stop()