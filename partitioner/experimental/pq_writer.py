import pyarrow as pa
import pyarrow.parquet as pq
import os

# Given a bucket and PyArrow Table, write the data to DISC/Local Memory.
# Optional: Specify an existing directory

# Notes: PyArrow has not implemented a Open File I/O so we reuse the old one.
# Add Bucket Flush

def store_row_partitions(buckets, table, dir=None):
    # Opens a file for writing only. Overwrites the file if the file exists.
    # If the file does not exist, creates a new file for writing.
    files = {}
    owd = os.getcwd()
    if dir:
        os.chdir(dir)

    # Initialize storage location
    for oid in buckets.keys():
        file_name = ("bin-Object-%s" % oid)
        # Delete old files if they exist, using w and b to write bytes
        files[oid] = open(file_name, 'wb')

    # Store tables into the file
    for oid, bucket in buckets.items():
        fp = files[oid]
        write_row_bucket(fp, bucket, table)

    # Close all respective files
    for oid in buckets.keys():
        files[oid].close()

    # Go back to default dir
    os.chdir(owd)
    return

# Stores bucket into an existing file.
# Given the file pointer, the rows, and table of data.
def write_row_bucket(file, row_list, table):
    # print(row_list)
    if not row_list:
        print(row_list)
        return
    else:
        # Gather the rows in the table
        t = table.take(row_list)
        records = t.to_batches() # Convert table to list of "RecordBatch Obj" (len =1)

    # Add the contents to the file as batches.

    # Initialize Writer/Stream with Schema
    sink = pa.BufferOutputStream()
    stream_writer = pa.ipc.new_stream(sink, records[0].schema)

    for batch in records:
        # store batch into buffer stream
        stream_writer.write_batch(batch)

    stream_writer.close()

    # Check how much data was retained
    buffer = sink.getvalue()

    # Write the batch into a file, and this automatically updates the file pointer.
    file.write(buffer)

    return

# ipc.new_stream determines the batch obj type. https://arrow.apache.org/docs/python/api/ipc.html