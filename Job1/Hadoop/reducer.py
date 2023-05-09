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

def emit_top_words(product_id, words):
    # Emitting the top words for a given product
    top_words = heapq.nlargest(TOP_WORDS_PER_PRODUCT, words, key=lambda k: words[k])
    for word in top_words:
        count = words[word]
        print(f'{product_id}\t{word}\t{count}')

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
                emit_top_words(ProductId, product_counts)

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
        current_product_words[ProductId] = set()

# Emitting the result for the last year
if current_year is not None:
    for ProductId in current_product_counts:
        product_counts = current_product_counts[ProductId]
        emit_top_words(ProductId, product_counts)