from .parser import SQLParser
from .skyhook import SkyhookRunner

class Query():
    """A class that represents a mutable SQL query object."""

    def __init__(self):        
        self.statement = ''

        self.options = {'cls'              : True,
                        'quiet'            : False,
                        'header'           : True,
                        'pool'             : 'tpchdata',
                        'num-objs'         : '2',
                        'oid-prefix'       : 'public',
                        'path_to_run_query': 'cd ~/skyhookdm-ceph/build/ && bin/run-query'}

        self.query = {'selection'  : [],
                      'projection' : [],
                      'table-name' : []}
        
        self.results = None

    def sql(self, statement):
        """Parses SQL statement and sets Query object parameters. 

        Arguments:
        statement -- A string parsed as an unvalidated SQL statement
        """
        self.statement = statement
        parsed = SQLParser.parse_query(statement)
        self.set_table_name(parsed['table-name'])
        self.set_projection(parsed['projection'])
        self.set_selection(parsed['selection'])

    def run(self):
        """A function that executes the Skyhook CLI command by calling the run-query binary."""
        self.results = SkyhookRunner.run_query(self.query, self.options)

    def lifetime(self, statement):
        """A function that performs a full SQL query execution. 

        Arguments:
        statement -- A string parsed as an unvalidated SQL statement
        """
        query = SQLParser.parse_query(statement)

        self.set_projection(query['projection'])
        
        self.set_selection(query['selection'])

        self.set_table_name(query['table-name'])

        self.create_sk_cmd()

        self.run()

    def set_selection(self, *values):
        """Sets the selection parameter for a query.

        Arguments:
        values -- Any number of strings of predicates of the form \"<comparison>,<operand>,<operand>\"
        """
        predicates = []
        for value in values:
            if not isinstance(value, str):
                raise TypeError("Value of predicate must be a string")
            if len(value.split()) != 3:
                raise ValueError("Expected predicate formatted: \"<comparison>,<operand>,<operand>\"")
            # TODO: Opertors belong in utils or SkyhookRunner? 
            operators = ['geq', 'leq', 'ne', 'eq', 'gt', 'lt', 'like']
            comparison_op = value.split()[1].strip(', ')
            if comparison_op not in operators:
                raise ValueError("Comparison operator \'{}\' is not in {}".format(comparison_op, operators))
            predicates.append(value)
            self.query['selection'] = predicates

    def set_projection(self, *values):
        """Sets the projection parameter for a query.

        Arguments:
        values -- Any number of strings of names of attributes
        """
        attributes = []
        for value in values:
            if not isinstance(value, str):
                raise TypeError("Value of attribute must be a string")
            attributes.append(value)
        self.query['projection'] = attributes

    def set_table_name(self, *values):
        """Sets the table name parameter for a query.

        Arguments:
        values -- Any number of strings of table names
        """
        table_names = []
        for value in values:
            if not isinstance(value, str):
                raise TypeError("Value of table name must be a string")
            table_names.append(value)
        self.query['table-name'] = table_names

    def set_option(self, option, value):
        """Sets the option to be the given value.

        Arguments:
        option -- The string name of the option to be changed
        value  -- The string value of the option to be set
        """
        if option not in self.options.keys():
            print("Error: Not an option")
            return
        self.options[str(option)] = value 

    def show_query(self):
        """A function that prints the current Query object."""
        print(self.query)

    def show_options(self):
        """A function that prints the current options being used."""
        print(self.options)

    def show_results(self):
        """A function that prints the results of the previously ran query."""
        print(self.results)

    def show_sk_cmd(self):
        """A function that prints the Skyhook CLI command representation of the query object."""
        sk_cmd = SkyhookRunner.create_sk_cmd(self.query, self.options)
        print(sk_cmd)

    def generate_pyarrow_dataframe(self):
        """An example of what may be done in the near future."""
        raise NotImplementedError
