# Import built-in file comparison library
import os
import filecmp # https://docs.python.org/3/library/filecmp.html
import shutil# shutil.rmtree() deletes a directory and all its contents.

import pyarrow as pa
import pandas as pd
from bucket_map import row_map
from main import pq_etl


# Helper Function: Empties a given directory
def init_test(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
    if not os.path.exists(directory):
        os.mkdir(directory)
    return

# Currently works well with Files that Exist. Empty Buckets aka blank files should be ignored.
def test_gen(file, schema, nrows, bucket_size, num_buckets, calc_dir, expect_dir):
    # Initialize the local storage
    init_test(calc_dir)
    # Call CSV Reader-like and populate calc directory
    pq_etl(file, schema, bucket_size, num_buckets, nrows, calc_dir)
    pq_etl(file, schema, bucket_size, num_buckets, nrows, expect_dir)
    # Based on KNOWN outputs

    # TEST 1: Compare Arrow Binaries on Disk 
    # Compare Computed Arrow Binaries on Disk with Expected Arrow Binary
    print("Compare Bucket 0 on Disk:", comp_files(calc_dir, expect_dir, "bin-Object-0"))
    print("Compare Bucket 1 on Disk:", comp_files(calc_dir, expect_dir, "bin-Object-1"))

    data_skyhook = pd.read_csv(file, sep='|', nrows=nrows, names=schema, header=0)
    df = pd.DataFrame(data_skyhook)
    table = pa.Table.from_pandas(df)
    nrows = table.num_rows
    mapping = row_map(data=data_skyhook, pk1="ORDERKEY", pk2="LINENUMBER", num_buckets=2, max_rows=nrows, max_size=bucket_size)

    # Convert the Pandas dataframe to an Arrow Table
    table = pa.Table.from_pandas(df)

    # TEST 2: Compare Logical Contents of Arrow Tables as DataFrames
    # Compare Computed Dataframe with Expected Dataframe
    print("Read-Write Bucket 0:", read_write_test(mapping, 0, "bin-Object-0", table))
    print("Read-Write Bucket 1:", read_write_test(mapping, 1, "bin-Object-1", table))

    return True

# Checks that if a csv is empty, no files should be created.
def test_errors(file, schema, nrows, bucket_size, num_buckets, calc_dir, expect_dir):
    init_test(calc_dir)
    check = pq_etl(file, schema, bucket_size, num_buckets, nrows, calc_dir)
    if check is False:
        return True # Passed the Test
    else:
        return False # Failed the Test

# A trivial test to check if two binaries are the same.
def comp_files(dir1, dir2, filename):
    # Given a file with the same name, and the locations of both.
    owd = os.getcwd()
    path1 = (owd + '/' + dir1 + '/' + filename)
    path2 = (owd + '/' + dir2 + '/' + filename)

    # print("Comparing", path1, "with", path2)

    # Get the paths of the two files.
    return filecmp.cmp(path1, path2)


# Test: Compare the buffer with the file contents as a dataframe.
def read_write_test(buckets, bucket_id, file, table):
    # Buffer 1: Result of CSV Reader 
    # Set up variables for the read_csv to match a schema
    
    # Finally use the maping and pick ONE bucket.
    # Gather the rows in the table that match the bucket_id
    if buckets[bucket_id] == []:
        bucket_table = 0
    else:
        # Convert the one bucket to a pandas dataframe.
        bucket_table = table.take(buckets[bucket_id])
        df_bucket = bucket_table.to_pandas()
        
    # Now we have the bucket as a dataframe.

    # Buffer 2: Read from File, checkout tst directory
    owd = os.getcwd()
    os.chdir('tst')

    # Check if file is empty
    if os.stat(file).st_size == 0:
        reader = 0
    else:
        # Extract contents into buffer
        fp = open(file, 'rb')
        buffer = fp.read()
        sink = pa.BufferReader(buffer)
        reader = pa.ipc.open_stream(sink)

    # Compare the two buffers (reader and bucket)

    # Edge case of an empty bucket/file.
    if bucket_table == 0  and reader == 0:
        print("Both are empty")
        test_result = True

    else:
        # Covert the Reader buffer into a PyArrow Table, then Pandas Dataframe.
        read_table = reader.read_all()
        df_reader = read_table.to_pandas()

        # Compare the two dataframes
        test_result = df_bucket.equals(df_reader)
        
    # return back to old directory
    os.chdir(owd)
    
    return test_result

# Add Edge Cases: Empty CSV, 0 Rows, inf Rows, Should not be making blank files. Error_bad_lines.
# Blank PK_2, Blank PK_1
# Init Test function: Wipes Directory

# Init Test cases
test_files = ['100-row-stripped.csv', 'empty.csv', 'bad', 'missing.csv', 'bad.csv']
bucket_limit = 2
values = [bucket_limit, 40, 100] # Rows per bucket
schema = ['ORDERKEY', 'PARTKEY', 'SUPPKEY', 'LINENUMBER', 'QUANTITY',
       'EXTENDEDPRICE', 'DISCOUNT', 'TAX', 'RETURNFLAG', 'LINESTATUS',
       'SHIPDATE', 'COMMITDATE', 'RECEIPTDATE', 'SHIPINSTRUCT', 'SHIPMODE',
       'COMMENT']
       
## Add Changes to Datatype to infer the correct type with Skyhook's metadta
# df['ORDERKEY'].astype('int')
   # ... #

target = 'obj'
test = 'tst'
rows_to_read = 100

# Run tests

### Tests for Error Handling ###

# Tests on an empty file
print("Test case 4: Empty file")
test_errors(test_files[1], schema, rows_to_read, values[0], bucket_limit, target, test)

# Test on non-file
print("Test case 5: Invalid file type")
test_errors(test_files[2], schema, rows_to_read, values[0], bucket_limit, target, test)

# Test on missing file
print("Test case 6: File doesn't exist")
test_errors(test_files[3], schema, rows_to_read, values[0], bucket_limit, target, test)

# If CSV Has error don't write anything.
print("Test case 6: File is not a valid csv")
test_errors(test_files[4], schema, rows_to_read, values[0], bucket_limit, target, test) 

# Test with No Flush
print("Test case 1: Normal file, with no flushing")
test_gen(test_files[0], schema, rows_to_read, values[2], bucket_limit, target, test)

# Test With Flush
print("Test case 2: Normal file, with flushing")
test_gen(test_files[0], schema, rows_to_read, values[1], bucket_limit, target, test)

print("Test case 3: Normal file, with empty buckets.")
# Test: Schema where an empty bucket exists. TODO IMPROVEMENTS. Cannot read from empty file.
test_gen(test_files[0], schema, rows_to_read, values[0], bucket_limit, target, test)



# TODO: Could make it iterate for the normal files as file[0] with values[i], and files[1:] with values[x].
# TODO: Turn the if/Else into a try/Except err()