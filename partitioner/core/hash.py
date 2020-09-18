# Applying the Jump Consistent Hash function
import jump
import sys
import numpy as np

# Generate a 64 bit key from two 32 bit values and the size of buckets.

def gen_hashbucket(key_a, key_b, num_buckets):
    key_a = int(key_a)
    key_b = int(key_b)

    # Merge the two values
    key_c = ((key_a << 32) | key_b)

    # Hash 
    bucket_id = jump.hash(key_c, num_buckets)
    return bucket_id
