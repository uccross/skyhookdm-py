from .query.engines import SkyhookRunQuery
from .query.parsers import SQLParser

def query_string(statement):
    """A function that returns the intermediate representation of a SQL statement

    Arguments:
    statement -- A SQL string 
    """
    return SQLParser.parse_query(statement)

def run(query):
    """A function that executes a Skyhook CLI command.
    
    Arguments:
    query -- Intermediate representation of a SQL statement
    """
    return SkyhookRunQuery.run_query(query)

def show_sk_cmd(query):
    """A function that returns the Skyhook CLI commmand of a SQL statement in SQLIR.

    Arguments:
    query -- Intermediate representation of a SQL statement
    """
    return SkyhookRunQuery.create_sk_cmd(query)
    