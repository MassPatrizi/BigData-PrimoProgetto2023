#!/usr/bin/env python3

"""mapper.py"""
import sys

for row in sys.stdin:
    row = row.strip().split(',')

    # Ignora la riga di intestazione
    if row[0] == 'Id':
        continue

    # Solo se row è di almeno 10 campi, printa userId, Numerator e Denominator
    if len(row) >= 10:
        userId = row[2]
        helpfulnessNumerator = row[4]
        helpfulnessDenominator = row[5]

        print(f'{userId}\t{helpfulnessNumerator}\t{helpfulnessDenominator}')
