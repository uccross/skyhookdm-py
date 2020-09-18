from hash import gen_hashbucket

# Given at at most two Primary Keys, the datafile, and rows.
# Mapping function, each row containing bucket_id
# Generates a dictionary mapping of every object to a bucket.
def row_map(data, pk1, num_buckets, max_rows,  pk2=None, max_size=None):

    # Disable flushing if user doesn't specify
    if max_size is None:
    # Setting max_size = max_rows, means it will never flush as a > a.
        max_size = max_rows

    # Add Check for pk2 = None
    buckets = {}
    row = 0

    # Number of times the buckets have been flushed.
    flush_num = 0 

    # Create a holder for the buckets using a dictionary
    for row in range(0, max_rows):
        # Get the values for the two keys, ORDERKEY and LINENUMBER.
        if pk1:
            key1 = data[pk1]
            key1 = key1.loc[row]
        else:
            return "Missing PK1"

        if pk2:
            key2 = data[pk2]
            key2 = key2.loc[row]
        else:
            key2 = 0
        # print("My keys are %d and %d" % (orderkey, linenumber))
        bucket_id = gen_hashbucket(key1, key2, num_buckets) # Change to num_buckets
        # print("This goes in bucket %d" % bucket_id)

        # Append the value to the existing list
        if bucket_id in buckets:
            # print("Append")
            buckets[bucket_id].append(row)
        else:
            # Create and Store in Bucket if item doesn't exist
            # print("Create")
            bucket_list = []
            bucket_list.append(row)
            buckets[bucket_id] = bucket_list

    # Return the bucket
    return buckets

# Testing function
def show_map(buckets):
    print(buckets.keys())
    for counter, (key, value) in enumerate(buckets.items(), 0):
        print(counter, key, value)