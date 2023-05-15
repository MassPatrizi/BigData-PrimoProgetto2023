#!/usr/bin/env python3

# TO-DO
# Devo ottenere una tabella ordinata con userId, apprezzamento
# L'apprezzamento è la somma dell'utilità di tutte le recensioni eseguite, diviso il numero di recensioni.
# L'utilità è il rapporto di helpfullnessNumerator e Denominator di una recensione. 

"""reducer.py"""
import sys

userId_utilities_dictionary = {}

# in questa fase produco il dizionario userId e lista di utilità
for line in sys.stdin:
    # Parsing the input line
    values = line.strip().split('\t')

    # Verifying that the line contains all the expected values
    if len(values) != 3:
        print(f"Illegible input: {line}", file=sys.stderr)
        continue

    # store values in str variables
    userId, helpfulnessNumerator, helpfulnessDenominator = values

    # compute utility, handles the division by zero
    if(int(helpfulnessDenominator) != 0):
        utility = int(helpfulnessNumerator) / int(helpfulnessDenominator)
    else:
        utility = float(0)
    
    # update the dictionary with the computed utility
    # if userId is in the dictionary, just append the new utility to the value, else create the list
    if(userId in userId_utilities_dictionary):
        userId_utilities_dictionary[userId].append(utility)
    else:
        userId_utilities_dictionary[userId] = [utility]
    
# adesso dobbiamo calcolare la media delle utilità
userId_appreciation_dictionary = {}

for userId in userId_utilities_dictionary.keys():
    userId_appreciation_dictionary[userId] = sum(userId_utilities_dictionary[userId]) / len(userId_utilities_dictionary[userId])

# ordina il dizionario in base al value
sorted_dict = dict(sorted(userId_appreciation_dictionary.items(), key=lambda x: -x[1]))

# print the output
for userId in sorted_dict.keys():
    print(f'{userId}\t{sorted_dict[userId]}')
