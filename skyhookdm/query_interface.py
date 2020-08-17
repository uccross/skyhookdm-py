from .query.engines import SkyhookRunQuery, EngineOptions, DatasetOptions
from .query.parsers import SQLParser

def query_string(statement):
    """A function that returns the intermediate representation of a SQL statement

    Arguments:
    statement -- A SQL string 
    """
    return SQLParser.parse_query(statement)

def engine_options(options=None):
    """A function that returns an EngineOptions object
    
    Arguments:
    options -- A dictionary of options and their values. If None, default options returned
    """
    return EngineOptions.get_options(options)

def dataset_options(dataset=None):
    return DatasetOptions.get_dataset(dataset)

def change_option(options):
    return NotImplemented

def run(query, engine_options, dataset_options):
    """A function that executes a Skyhook CLI command.
    
    Arguments:
    query -- Intermediate representation of a SQL statement
    engine_options -- Engine-specific options for executing the query
    dataset_options -- Dataset-specific options for executing the query 
    """
    return SkyhookRunQuery.run_query(query, engine_options, dataset_options)

def show_sk_cmd(query, engine_options, dataset_options):
    """A function that returns the Skyhook CLI commmand of a SQL statement in SQLIR.

    Arguments:
    query -- Intermediate representation of a SQL statement
    """
    print(SkyhookRunQuery.create_sk_cmd(query, engine_options, dataset_options))
    

def show_options():
    """A function that prints the current options being used."""
    return NotImplemented
