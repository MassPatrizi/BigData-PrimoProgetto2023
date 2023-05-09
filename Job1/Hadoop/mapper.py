#!/usr/bin/env python3

"""mapper.py"""
import sys
import csv
import datetime

def extract_year_from_unix_time(unix_time):
    try:
        unix_time = int(unix_time)
        date = datetime.datetime.fromtimestamp(unix_time)
        year = date.year
        return year
    except ValueError:
        print("Invalid Unix timestamp:", unix_time)
        return None

for row in sys.stdin:
    row = row.strip().split(',')

    # Ignora la riga di intestazione
    if row[0] == 'Id':
        continue

    # Estrarre i dati delle recensioni
    if len(row) >= 10:
        ProductId = row[1]
        Time = row[7]
        Text = row[9]

        # Estrarre l'anno dalla data
        year = extract_year_from_unix_time(Time)

        print(f'{ProductId}\t{year}\t{Text}')


