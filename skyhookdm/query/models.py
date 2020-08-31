class SQLIR():
    """A class that represents a mutable SQL query object."""

    def __init__(self):        
        self.ir = {'selection'  : [],
                   'projection' : [],
                   'table-name' : []}

    def set_selection(self, *values):
        """Sets the selection parameter for a query.

        Arguments:
        values -- Any number of strings of predicates of the form \"<operand>,<comparison>,<operand>\"
        """
        predicates = []
        for value in values:
            if not isinstance(value, str):
                raise TypeError("Value of predicate must be a string")
            if value ==  '': 
                continue
            if len(value.split()) != 3:
                raise ValueError("Expected predicate formatted: \"<operand>,<comparison>,<operand>\"")
            comparison_op = value.split()[1].strip(', ')
            supported_ops = ['geq', 'leq', 'ne', 'eq', 'gt', 'lt', 'like']
            if comparison_op not in supported_ops:
                raise ValueError("Comparison operator \'{}\' is not a supported operation".format(comparison_op))
            predicates.append(value)
            self.ir['selection'] = predicates

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
        self.ir['projection'] = attributes

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
        self.ir['table-name'] = table_names

    def set_create_index(self, *values):
        pass

    def set_describe_table(self, *values):
        pass

    def show_query(self):
        """A function that prints the current Query object."""
        print(self.ir)
