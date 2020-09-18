import os

# Given a bucket and PyArrow Table, write the data to DISC/Local Memory.
# Optional: Specify an existing directory

# Add Bucket Flush
# Add compatibility with PyArrow Writer
# Redo this using PyArrow's I/O

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
        # write Bytes not String

    # Store tables into the file
    for oid, row_list in buckets.items():
        # Create a subset of the table that contains all rows.
        print("Selecting", row_list)
        t = table.take(row_list)
        records = t.to_batches() # Convert table to list of "RecordBatch Obj" (len =1)

        print(len(records))

        # ADD BATCHING
        for batch in records:
            # Add the contents to the file.
            print("Added %d bytes" % (batch.nbytes))
            files[oid].write(str(batch))
    # Close all files
    for oid in buckets.keys():
        files[oid].close()
    return
