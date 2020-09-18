from hash import gen_hashbucket

# Takes a PyArrow Table and extracts their values into a list
def get_key_list(table, key):
     if pk:
        pkey = table.column(key)
        return pkey
    return None

# Given at at most two Primary Keys, the datafile, and rows.
# Mapping function, each row containing bucket_id
# Generates a dictionary mapping of every object to a bucket.
def row_map(table, pk1, num_buckets, max_rows,  pk2=None):

    buckets = {}
    isSingle = False
    current_row = 0

    key1 = get_key_list(table, pk1)
    key2 = get_key_list(table, pk2)
    
    if key1 is None:
        raise("Missing Primary Key")
    if key2 is None:
        isSingle = True
    
    # Create a holder for the buckets using a dictionary
    for row in range(0, max_rows):
        if isSingle:
            # Default key 2 to be 0, so it doesn't affect the hash
            bucket_id = gen_hashbucket(key1[row], 0, num_buckets)
        else:
            bucket_id = gen_hashbucket(key1[row], key2[row], num_buckets)

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