#!/usr/bin/env python3

"""reducer.py"""
import sys
import heapq

# Defining the number of top products and top words per product
TOP_PRODUCTS = 10
TOP_WORDS_PER_PRODUCT = 5

current_year = None
current_product_counts = {}
current_product_words = {}

def emit_top_words(product_id, words, year):
    # Emitting the top words for a given product
    top_words = heapq.nlargest(TOP_WORDS_PER_PRODUCT, words, key=lambda k: words[k])
    for word in top_words:
        count = words[word]
        output_data.append((year, product_id, word, count))

output_data = []  # List to store the (year, product, word, count) tuples

for line in sys.stdin:
    # Parsing the input line
    values = line.strip().split('\t')

    # Verifying that the line contains all the expected values
    if len(values) != 3:
        print(f"Illegible input: {line}", file=sys.stderr)
        continue

    ProductId, year, Text = values

    # If a new year is encountered
    if current_year != year:
        # If there was a year being processed, emit the result
        if current_year is not None:
            for ProductId in current_product_counts:
                product_counts = current_product_counts[ProductId]
                emit_top_words(ProductId, product_counts, current_year)

        # Reset the variables for the new year
        current_year = year
        current_product_counts = {}
        current_product_words = {}

    # Update the counts for the current product
    if ProductId in current_product_counts:
        product_counts = current_product_counts[ProductId]
        for word in Text.split():
            if len(word) >= 4:
                product_counts[word] = product_counts.get(word, 0) + 1
    else:
        current_product_counts[ProductId] = {}
        current_product_words[ProductId] = {}

    # Update the (word, count) tuple for the current product
    product_words = current_product_words[ProductId]
    for word in Text.split():
        if len(word) >= 4:
            product_words[word] = product_words.get(word, 0) + 1

# Emitting the result for the last year
if current_year is not None:
    for ProductId in current_product_counts:
        product_counts = current_product_counts[ProductId]
        emit_top_words(ProductId, product_counts, current_year)

# Sorting the output_data by year
output_data_sorted = sorted(output_data, key=lambda x: x[0])

# Print the final output
previous_year = None
rows_per_year = {}  # Dizionario per tenere traccia delle righe per ogni ProductId nell'anno corrente
max_rows_per_year = 50  # Numero massimo di righe per anno
max_rows_per_ProductId = 5  # Numero massimo di righe per ogni ProductId
max_products_per_year = 10
products_per_year = 0

for year, ProductId, word, count in output_data_sorted:
    if year != previous_year:
        if previous_year is not None:
            # Verifica se abbiamo raggiunto il numero massimo di righe per l'anno corrente
            if sum(rows_per_year.values()) >= max_rows_per_year:
                rows_per_year = {}
                products_per_year = 0
                continue

            # Verifica se abbiamo raggiunto il numero massimo di ProductId distinti per l'anno corrente
            if products_per_year >= max_products_per_year:
                rows_per_year = {}
                products_per_year = 0
                continue

            # Stampa i ProductId mancanti per l'anno corrente
            missing_ProductIds = max_products_per_year - products_per_year
            for ProductId, count in rows_per_year.items():
                if missing_ProductIds == 0:
                    continue
                if count < max_rows_per_ProductId:
                    print(f'\t{ProductId}\tMissing\t0')
                    missing_ProductIds -= 1
        
        print(f'{year}:')
        previous_year = year
        rows_per_year = {}  # Reset del conteggio delle righe per l'anno corrente
        products_per_year = 0
    
    if ProductId not in rows_per_year and products_per_year < max_products_per_year:
        products_per_year += 1
        rows_per_year[ProductId] = 1
        print(f'\t{ProductId}\t{word}\t{count}')
    elif ProductId in rows_per_year and rows_per_year[ProductId] < max_rows_per_ProductId:
        rows_per_year[ProductId] += 1
        print(f'\t{ProductId}\t{word}\t\t{count}')



