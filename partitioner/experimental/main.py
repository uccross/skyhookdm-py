import os
# Performs an ETL on a csv file to write into skyhook as a pyarrow table.
import pyarrow as pa
import pandas as pd
from core.bucket_map import row_map, show_map
from core.writer import store_row_partitions, store_col_partitions
from core.csv_reader import generate_table_row, generate_table_col

# Set up variables for the read_csv to match a schema
# Returns true if successful, otherwise returns false
# Takes in an input file, bucket_size, number of objects, rows to read from file, and storage directory (or pool)
""" Sample Input Schema for reference 
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
        print("Invalid, Schema contains no keys")
        return False


    # Change method depending on row or column
    if type is 'row':
        # Returns a PyArrow Table following the specs provided in the schema
        table = generate_table_row(file, input_schema, nrows)
        # Pick a maximum size for a bucket
        # Change function to work for PyArrow Tables not Dataframes.
        mapping = row_map(data=table, pk1=key1, pk2=key2, num_buckets=num_buckets, max_rows=nrows)
        # Store buckets according to the mappings.
        store_row_partitions(buckets=mapping, table=table, max_size=max_bucket_size, dir=directory)

    if type is 'col':
        list_tables = []
        list_table = generate_table_col(file, input_schema, nrows)
        # Direct store of 1:1, each column goes in one bucket. No mapping required
        for i, item in enumerate(list_table):
            store_col_partitions(table, 'Object-' + i, dir=directory)

    return True

'''
Notes: I wanted to recycle generate table, but PyArrow.Table.select(column_name) isn't functioning.
As a result, I needed to split the columns inside the generation call.

Found Issue: PyArrow on Collab is default to 0.14.1, since my system is ver 1.0.0 it should be fine.
Will add a second "Optimized" version for users with pyarrow version 1.0.0
'''