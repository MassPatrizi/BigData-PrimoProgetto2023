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

# Create the Spark session
spark = SparkSession.builder.appName("ReviewsAnalysisJob2").getOrCreate()

# Use the Spark session to create RDD from the input data
input_RDD = spark.sparkContext.textFile(input_filepath).cache()

# remove header
header = input_RDD.first()
input_RDD = input_RDD.filter(f=lambda line: line != header)

# Run transformations asking them directly to the RDDs
# create a RDD splitting the csv by ";"
reviews_RDD = input_RDD.map(f=lambda line: line.strip().split(";"))
# create an RDD mapping only uderID, Num e Den
user_num_den_RDD = reviews_RDD.map(f=lambda line: (line[2], line[4], line[5]))
# create and RDD mapping userID, utility
user_utility_RDD = user_num_den_RDD.map(f=lambda line: (line[0], float_division(float(line[1]),float(line[2]))))
# Run the final action / actions asking them directly to the RDDs


# Write down the output
user_utility_RDD.saveAsTextFile(output_filepath)

# Chiudi la sessione Spark
spark.stop()