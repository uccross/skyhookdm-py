import os
# Performs an ETL on a csv file to write into skyhook as a pyarrow table.
import pyarrow as pa
import pandas as pd
from core.bucket_map import row_map, show_map
from core.writer import store_row_partitions, store_col_partitions
from core.reader import pa_dump
from core.csv_reader import generate_table

# Set up variables for the read_csv to match a schema
# Returns true if successful, otherwise returns false
# Takes in an input file, bucket_size, number of objects, rows to read from file, and storage directory (or pool)
""" Sample Input Schema for refference 
input_schema = [('ORDERKEY', 'int32', 1), ('PARTKEY', 'int32', 0),
                 ('SUPPKEY', 'int32', 0), ('LINENUMBER', 'int32', 1),
                 ('QUANTITY', 'float', 0),('EXTENDEDPRICE', 'double', 0),
                 ('DISCOUNT', 'float', 0), ('TAX', 'double', 0), 
                 ('RETURNFLAG', 'char', 0), ('LINESTATUS', 'char', 0),
                 ('SHIPDATE', 'date', 0), ('COMMITDATE', 'date', 0),
                 ('RECEIPTDATE', 'date', 0), ('SHIPINSTRUCT', 'string', 0),
                 ('SHIPMODE', 'string', 0), ('COMMENT', 'string', 0)]
"""
def pq_etl(type, file, input_schema, max_bucket_size, num_buckets, nrows=100, directory='obj'):

    key1, key2 = get_keys(input_schema)
    if (key1 is None) and (key2 is None):
        raise("Invalid, Schema contains no keys")
    table = generate_table(file, input_schema, nrows)
    if len(keys) == 0:
        
    # Map the function
    if type = 'row':
        # Pick a maximum size for a bucket
        # Change function to work for PyArrow Tables not Dataframes.
        mapping = row_map(data=table, pk1=key1, pk2=key2, num_buckets=num_buckets, max_rows=nrows)
        # Store buckets according to the mappings.
        store_row_partitions(buckets=mapping, table=table, max_size=max_bucket_size, dir=directory)

    if type = 'col':
        raise("Feature not Implemented")
        # Direct store of 1:1, each column goes in one bucket. No mapping required
        #store_col_partitions(table=table, dir=directory)



    # Print the file to check contents
    # pa_dump(buckets=mapping, dir=directory)
    return True