import os
# Performs an ETL on a csv file to write into skyhook as a pyarrow table.
import pyarrow as pa
import pandas as pd
from core.bucket_map import row_map, show_map
from core.writer import store_row_partitions, store_col_partitions
from core.csv_reader import generate_table
'''
This is an optimized version for users running PyArrow 1.0.0 or higher.
'''
# Set up variables for the read_csv to match a schema
# Returns true if successful, otherwise returns false
# Takes in an input file, bucket_size, number of objects, rows to read from file, and storage directory (or pool)
""" Sample Input Schema for reference. Keys are not specified in schema
input_schema = [('ORDERKEY', 'int32'), ('PARTKEY', 'int32'),
                 ('SUPPKEY', 'int32'), ('LINENUMBER', 'int32'),
                 ('QUANTITY', 'float'),('EXTENDEDPRICE', 'double'),
                 ('DISCOUNT', 'float'), ('TAX', 'double'),
                 ('RETURNFLAG', 'char'), ('LINESTATUS', 'char'),
                 ('SHIPDATE', 'date'), ('COMMITDATE', 'date'),
                 ('RECEIPTDATE', 'date'), ('SHIPINSTRUCT', 'string'),
                 ('SHIPMODE', 'string'), ('COMMENT', 'string')]
"""

# Partitioner takes in a table and keys and buckets (if row), and location to store.
# Returns whether the operation suceeded or failed.
def skyhook_partitioner(type, table, max_partition_size, npartitions, nrows=100, storage='obj', key1=None, key2=None):
    # Change method depending on row or column
    if type is 'row':
        if (key1 is None) and (key2 is None):
            print("Invalid, Schema contains no keys")
        return False
        # Pick a maximum size for a bucket
        # Change function to work for PyArrow Tables not Dataframes.
        mapping = row_map(data=table, pk1=key1, pk2=key2, num_buckets=npartitions, max_rows=nrows)
        # Store buckets according to the mappings.
        store_row_partitions(buckets=mapping, table=table, max_size=max_partition_size, dir=storage)

    if type is 'col':
        for i, name in enumerate(table.column_names):
            # Get the column
            names = []
            names.append(name)
            new = table.select(names)
            # Store the column into a location
            store_col_partitions(new, 'Object-' + i, dir=storage)
            names.clear()
    return True

# Main Program that extracts data from a csv, transforms it to PyArrow and the paritions it to store.
def pq_etl(type, file, input_schema, max_partition_size, npartitions, nrows=100, directory='obj', key1=None, key2=None):
    table = generate_table(file, input_schema, nrows)
    # Using the table, break it down into partitions and store.
    status = skyhook_partitioner(type, table, max_partition_size, npartitions, nrows, storage)
    return status
