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
    #row = next(csv.reader([line]))
    row = row.strip().split(',')
    
    if 'Id,ProductId,' in row:
        continue

    # Estrarre i dati delle recensioni
    if len(row)==10:
        _, ProductId, _, _, _, _, _, Time, _, Text = row

    # Estrarre l'anno dalla data
    year = extract_year_from_unix_time(Time)

    print(f'{ProductId}\t{year}\t{Text}')

