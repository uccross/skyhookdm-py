import os
import pyarrow as pa
import pandas as pd

def pa_dump(buckets, dir=None):
    files = {}
    owd = os.getcwd()
    if dir:
        os.chdir(dir)

    for oid in buckets.keys():
        file_name = ("bin-Object-%s" % oid)
        # Open files to read if they exist.
        files[oid] = open(file_name, 'rb')

        # Read into buffer and print out as pandas df
        my_file = files[oid]
        buffer = my_file.read() # args
        # print(buffer)
        sink = pa.BufferReader(buffer)
        # print(sink)
        # print(buffer)
        reader = pa.ipc.open_stream(sink) # This reader is a binary of the RecordBatchStreamReader Obj
        # print(reader.schema)
        # print()
        # print(reader)

        # Break down the Recordbatches
        table = reader.read_all()
        # print(type(table))
        # print(table)
        output = table.to_string()
        df = table.to_pandas()
        # print(output)
        # print(df)
        files[oid].close()
    # Go back to default dir
    os.chdir(owd)
    return