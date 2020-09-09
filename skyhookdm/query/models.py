class SQLIR():
    """A class that represents a mutable SQL query object."""

    def __init__(self):        
        self.ir = {'selection'  : [],
                   'projection' : [], 
                   'table-name' : [],
                   'attributes' : [],
                   'options'    : []}

    def set_selection(self, *args):
        """Sets the selection parameter for a query.

        Arguments:
        args -- Any number of strings representing predicates of the form \"<operand>,<comparison>,<operand>\"
        """
        if self._is_string(args):
            predicates = []
            for value in args:
                if value ==  '': 
                    continue
                if len(value.split(',')) != 3:
                    raise ValueError("Expected predicate formatted: \"<operand>,<comparison>,<operand>\"")
                comparison_op = value.split(',')[1].strip()
                supported_ops = ['geq', 'leq', 'ne', 'eq', 'gt', 'lt', 'like']
                if comparison_op not in supported_ops:
                    raise ValueError("Comparison operator \'{}\' is not a supported operation".format(comparison_op))
                predicates.append(value)
                self.ir['selection'] = predicates

    def set_projection(self, *args):
        """Sets the projection parameter for a query.

        Arguments:
        args -- Any number of strings representing names of attributes
        """
        if self._is_string(args):
            self.ir['projection'] = list(args)

    def set_table_name(self, *args):
        """Sets the table name parameter for a query.

        Arguments:
        args -- Any number of strings representing table names
        """
        if self._is_string(args):
            self.ir['table-name'] = list(args)

    def set_create_index(self, *args):
        """Sets the create index query options and table name parameter.

        Arguments:
        args -- Any number of strings representing table names
        """
        if self._is_string(args):
            self.ir['options'] = ['index-create', 'index-cols']
            self.ir['attributes'] = list(args) 

    def set_describe_table(self, *args):
        """Sets the describe table query options and table name parameter.

        Arguments: 
        args -- Any number of strings representing table names
        """
        if self._is_string(args):
            self.ir['options'] = ['header', 'limit']
            self.ir['table-name'] = list(args)

    def show_query(self):
        """A function that prints the current Query object."""
        print(self.ir)

    @staticmethod
    def _is_string(names):
        for name in names:
            if not isinstance(name, str):
                raise TypeError("Value of name must be a string")
        return True
