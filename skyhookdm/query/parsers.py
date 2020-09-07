import sqlparse
from sqlparse.tokens import Keyword, DML, DDL
from sqlparse.sql import IdentifierList, Identifier, Where, Parenthesis, Comparison

from .models import SQLIR

class SQLParser():
    """A class that parses SQL statements."""

    @classmethod
    def parse_query(cls, raw_query):
        """A function that parses a SQL string into a dictionary representation. 

        Arguments:
        raw_query -- A string SQL statement

        Returns:
        query -- A dictionary representation of a SQL string
        """
        try:
            assert isinstance(raw_query, str)
        except AssertionError as error:
            print("Error: Cannot parse non-string {}".format(raw_query, error))
            return

        def parse_identifiers(tokenized):
            identifiers =  []
            for i in tokenized:
                if isinstance(i, Identifier):
                    identifiers.append(str(i))
            return identifiers

        def parse_clauses(tokenized):
            def parse_where_clause(tokenized):
                allowed_ops = ('>=', '<=', '!=', '<>', '=', '>', '<', 'like')

                operator_strs = {allowed_ops[0] : 'geq',
                                 allowed_ops[1] : 'leq',
                                 allowed_ops[2] : 'ne',
                                 allowed_ops[3] : 'ne',
                                 allowed_ops[4] : 'eq',
                                 allowed_ops[5] : 'gt',
                                 allowed_ops[6] : 'lt',
                                 allowed_ops[7] : 'like'}

                where_tokens = []
                for item in tokenized.tokens:
                    if isinstance(item, Where):
                        for i in item.tokens:
                            token = []
                            if i.ttype is Keyword and item.value.upper() == 'AND':
                                token = [] 
                            if isinstance(i, Comparison):
                                token.append(str(i.left))
                                operation = str(i).split(" ")[1]
                                if operation.lower() in allowed_ops:
                                    token.append(operator_strs[operation.lower()])
                                token.append(str(i.right))
                                where_tokens.append(','.join(token))
                return where_tokens

            def parse_from_clause(tokenized):
                    from_seen = False
                    for item in tokenized.tokens:
                        if from_seen:
                            if item.ttype is Keyword:
                                return
                            else:
                                yield item
                        elif item.ttype is Keyword and item.value.upper() == 'FROM':
                            from_seen = True

            def parse_select_clause(tokenized):
                select_seen = False
                for item in tokenized.tokens:
                    if select_seen:
                        if item.ttype is Keyword:
                            return
                        else:
                            yield item
                    elif item.ttype is DML and item.value.upper() == 'SELECT':
                        select_seen = True                

            def extract_identifiers(token_stream):
                for item in token_stream:
                    if isinstance(item, IdentifierList):
                        for identifier in item.get_identifiers():
                            yield identifier.get_name()
                    elif isinstance(item, Identifier):
                        yield item.get_name()

            # Determine if DDL of DML 
            sqlir = SQLIR()
            if tokenized[0].ttype is DDL and tokenized[0].value.upper() == 'CREATE': # TODO: @Matthew Also check for 'INDEX'
                sqlir.set_create_index(*parse_identifiers(tokenized))
                return sqlir
            elif tokenized[0].ttype is Keyword and tokenized[0].value.upper() == 'DESCRIBE': # TODO: @Matthew Also check for 'TABLE'
                sqlir.set_describe_table(*parse_identifiers(tokenized))
                return sqlir
            elif tokenized[0].ttype is DML and tokenized[0].value.upper() == 'SELECT':
                select_stream = parse_select_clause(tokenized)
                projection_ids = list(extract_identifiers(select_stream))

                from_stream = parse_from_clause(tokenized)
                table_ids = list(extract_identifiers(from_stream))

                selection_ids = parse_where_clause(tokenized)

                sqlir.set_projection(*projection_ids)
                sqlir.set_selection(*selection_ids)
                sqlir.set_table_name(*table_ids)

                return sqlir
            else:
                raise SyntaxError("Query must be supprt SQL syntax (SELECT, CREATE INDEX, DESCRIBE TABLE)")

        sql_statement = sqlparse.split(raw_query)[0]
        
        tokenized = sqlparse.parse(sql_statement)[0]

        return parse_clauses(tokenized)

