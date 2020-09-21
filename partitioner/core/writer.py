import os

# Given a bucket and PyArrow Table, write the data to DISC/Local Memory.
# Optional: Specify an existing directory

# TODOs
# Add Bucket Flush
# Add Rados support
# Add compatibility with PyArrow Writer
# Redo this using PyArrow's I/O (unsupported/not developed)

def store_row_partitions(buckets, table, dir=None, max_rows=-1):
    # Opens a file for writing only. Overwrites the file if the file exists.
    # If the file does not exist, creates a new file for writing.
    files = {}
    if dir:
        os.chdir(dir)

    for oid in buckets.keys():
        file_name = ("Object-%s" % oid)
        # Delete old files if they exist.
        files[oid] = open(file_name, 'a') # files[oid] = new RecordBatchWriter(file_name)

    # Store tables into the file
    for oid, row_list in buckets.items():
        # Create a subset of the table that contains all rows.
        t = table.take(row_list)
        records = t.to_batches() # Convert table to list of "RecordBatch Obj" (len =1)
        # ADD BATCHING
        for batch in records:
            # Add the contents to the file.
            files[oid].write(str(batch))

    # Close all files
    for oid in buckets.keys():
        files[oid].close()
    return

# Write a PyArrow table into a file. File location must be specified
def store_col_partitions(table, file_name, dir=None):
    # Create an I/O
    file = open(file_name, 'a')

    records = table.to_batches() # Convert table to list of "RecordBatch Obj" (len =1)

    # ADD BATCHING
    for batch in records:
        # Add the contents to the file.
        file.write(str(batch))

    # Close file
    file.close()