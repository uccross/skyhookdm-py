import pyarrow as pa
import pyarrow.parquet as pq
import rados, os

# Given a bucket and PyArrow Table, write the data to Skyhook.
# Notes: PyArrow has not implemented a Open File I/O so we reuse the old one.
# Add Bucket Flush

def make_connection(pool):
    #Create Handle Examples. TODO: Fill in with right params
    cluster = rados.Rados(conffile='ceph.conf')
#   cluster = rados.Rados(conffile=sys.argv[1])
#   cluster = rados.Rados(conffile = 'ceph.conf', conf = dict (keyring = '/path/to/keyring'))

    # Attempt connection to ceph
    print "\nlibrados version: " + str(cluster.version())
    print "Will attempt to connect to: " + str(cluster.conf_get('mon initial members'))

    cluster.connect()
    print "\nCluster ID: " + cluster.get_fsid()

    print "\n\nCluster Statistics"
    print "=================="
    cluster_stats = cluster.get_cluster_stats()

    for key, value in cluster_stats.iteritems():
        print key, value

    return cluster.open_ioctx(pool)

def pq_store_bucket_skyhook(buckets, table, pool=skyPy):
    
    # Check if the Pool exists
   ioctx = make_connection(pool)

    # # Initialize storage location with ioctx
    # for oid in buckets.keys():
        # # Generate a pool_id, similar to the file pointer.
        # file_name = ("bin-Object-%s" % oid)
        # ioctx = cluster.open_ioctx2(file_name)

    # Store tables into skyhook-ceph
    for oid, bucket in buckets.items():
        fp = files[oid]
        write_row_bucket(ioctx, fp, bucket, table)


    return

# Stores bucket into skyhook.
# Given the obj name, the rows, and table of data
def write_row_bucket(ioctx, object, row_list, table):
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
        # store batch into skyhook
        ioctx.write_full(object, stream_writer)

    # Close the buffer
    stream_writer.close()
    return

# ipc.new_stream determines the batch obj type. https://arrow.apache.org/docs/python/api/ipc.html