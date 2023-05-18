#!/usr/bin/env python3

"""reducer.py"""
import sys
import heapq
import re
import time

reducer_start_time = time.time()

# Defining the number of top products and top words per product
TOP_PRODUCTS = 10
TOP_WORDS_PER_PRODUCT = 5

current_year = None
current_product_counts = {}
current_product_reviews = {}

def emit_top_words(product_id, words, year, reviews):
    # Emitting the top words for a given product
    top_words = heapq.nlargest(TOP_WORDS_PER_PRODUCT, words, key=lambda k: words[k])
    for word in top_words:
        cleaned_word = re.sub(r'[^a-zA-Z]', '', word)  # Applica la regex per rimuovere caratteri non alfabetici
        if len(cleaned_word) >= 4:
            count = words[word]
            output_data.append((year, product_id, cleaned_word, count, reviews))


output_data = []  # List to store the (year, product, word, count, reviews) tuples

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
                product_reviews = current_product_reviews[ProductId]
                emit_top_words(ProductId, product_counts, current_year, product_reviews)

        # Reset the variables for the new year
        current_year = year
        current_product_counts = {}
        current_product_reviews = {}

    # Update the counts for the current product
    if ProductId in current_product_counts:
        product_counts = current_product_counts[ProductId]
        for word in Text.split():
            if len(word) >= 4:
                product_counts[word] = product_counts.get(word, 0) + 1
    else:
        current_product_counts[ProductId] = {}
        current_product_reviews[ProductId] = 0

    # Update the (word, count) tuple for the current product
    current_product_reviews[ProductId] += 1
    product_counts = current_product_counts[ProductId]
    for word in Text.split():
        if len(word) >= 4:
            product_counts[word] = product_counts.get(word, 0) + 1


# Emitting the result for the last year
if current_year is not None:
    for ProductId in current_product_counts:
        product_counts = current_product_counts[ProductId]
        product_reviews = current_product_reviews[ProductId]
        emit_top_words(ProductId, product_counts, current_year, product_reviews)

# Sorting the output_data by year and reviews in descending order
output_data_sorted = sorted(output_data, key=lambda x: (x[0], x[4]), reverse=True)

# Print the final output
previous_year = None
rows_per_year = {}
max_rows_per_year = 50
max_rows_per_ProductId = 5
max_products_per_year = 10
products_per_year = 0

for year, ProductId, word, count, reviews in output_data_sorted:
    if year != previous_year:
        if previous_year is not None:
            if sum(rows_per_year.values()) >= max_rows_per_year:
                rows_per_year = {}
                products_per_year = 0
                continue
            if products_per_year >= max_products_per_year:
                rows_per_year = {}
                products_per_year = 0
                continue
        
        print(f'{year}:')
        previous_year = year
        rows_per_year = {}
        products_per_year = 0
    
    if ProductId not in rows_per_year and products_per_year < max_products_per_year:
        products_per_year += 1
        rows_per_year[ProductId] = 1
        print(f'\t{ProductId}\t{reviews}\t\t{word}\t{count}')
    elif ProductId in rows_per_year and rows_per_year[ProductId] < max_rows_per_ProductId:
        rows_per_year[ProductId] += 1
        print(f'\t{ProductId}\t{reviews}\t\t{word}\t{count}')

reducer_end_time = time.time()
print(f'Reducer end time: {reducer_end_time - reducer_start_time} secondi')
