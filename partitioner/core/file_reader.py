import os
# Performs an ETL on a csv file to write into skyhook as a pyarrow table.
import pyarrow as pa
import pandas as pd

"""
Takes in a list of tuples containing the column name
datatype, and whether they are keys
"""

def get_names(schema):
    names = []
    for (name, _,) in schema:
        names.append(name)
    return names   

# Datatype codes, add on as needed. Reflects the Skyhook Metadata Wrapper
codes = {
    0: 'int32',
    1: 'float',
    2: 'double',
    3: 'char',
    4: 'string',
    5: 'date'
}

def enforce_datatype(df, code):

    # If it's a supported datatype
    if code not in codes.values():
        raise("The datatype you are looking for is currently not supported")

    ### Numeric values
    if code == 'int32':
        # Cast the dataframe to an int32
        df = df.astype('int32')

    if code == 'float16':
        # Cast the dataframe to an float16
        # We use the numpy type compatible with PyArrow
        df = df.astype(np.float16)

    if code == 'double':
        # Cast the dataframe to a double (float64)
        df = df.astype(np.float64)

    ### Lexical Values
    if code == 'char':
        # Cast the dataframe to an unsigned int8 
        # df = df.astype(np.uint8) # not supported due the orignal being too small
        # df = df.astype(np.uint8) # Same issue

        # A char is a string with one character, a string is an array.
        # Therefore a char can be represented as an array of one letter.
        # or String of Size 1.
        df = df.astype(str) # Test this to see if Skyhook understands it !!!

    if code == 'string':
        # Cast the dataframe to a python string
        df = df.astype(str)

    if code == 'date':
        # default date is PST
        df = df.astype('datetime64[ns, US/Pacific]')

    # Return back the casted dataframe
    return df

# Set up variables for the read_csv to match a schema
# Returns a PyArrow Table following the datatypes
def generate_table_(file, schema, max_bucket_size):

    # Error Handling
    if not os.path.exists(file):
        # Raise Error for File not Found
        print("File not Found")
        return False

    if not os.path.isfile(file):
        # Raise Error for Invalid type
        print("File specified is not a file")
        return False

    if os.stat(file).st_size == 0:
        # Raise Error for empty csv
        # raise ValueError('Empty File') # This ends all tests when I raise error.

        # Want to print error to console and return False.
        print("File is blank")
        return False

    # Get the names for the schema. Don't want (tuple) to be the name.
    col_names = get_names(schema)

    # Check if the file is a json, csv, or unsupported
    extension = os.path.splitext(file)[1][1:]
    print(extension)
    if extension is 'json':
        try:
            # Parses the file using | as a separator and reads nrows, following the schema given.
            data_skyhook = pd.read_json(file, sep='|', nrows=nrows, names=col_names, header=0, error_bad_lines=True)
        except pd.errors.ParserError as err:
            print(err)
            return False

    if extension is 'csv':
        try:
            # Parses the file using | as a separator and reads nrows, following the schema given.
            data_skyhook = pd.read_csv(file, sep='|', nrows=nrows, names=col_names, header=0, error_bad_lines=True)
        except pd.errors.ParserError as err:
            print(err)
            return False

    # Unsupported
    if extension is not 'json' and extension is not 'csv':
        print("File Type is not supported")
        return False

    # Load the Data into a pandas dataframe
    df = pd.DataFrame(data_skyhook)

    # No Datatypes specified
    if schema is None:
        # Convert to a table
        table = pa.Table.from_pandas(df)
        return table

    # Otherwise cast according to the schema
    # Cast dataframe to match desired Skyhook Schema
    for (column, datatype) in schema:
        col = df[[column]] # make it a dataframe by encasing another []
        # enforce Datatype on the Dataframe
        new = enforce_datatype(col, datatype)
        # Replace the column with the casted version
        df[[column]] = new

    # Convert the Pandas dataframe to an Arrow Table
    table = pa.Table.from_pandas(df)
    # Return the converted table
    return table