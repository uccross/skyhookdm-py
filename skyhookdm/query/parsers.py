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

        def parse_describe_table(tokenized):
            pass

        def parse_create_index(tokenized):
            pass 

        def parse_clauses(tokenized):
            def parse_where_clause(tokenized):
                allowed_ops = ['>=', '<=', '!=', '<>', '=', '>', '<', 'LIKE']

                operator_strs = {allowed_ops[0] : 'geq',
                             allowed_ops[1] : 'leq',
                             allowed_ops[2] : 'ne',
                             allowed_ops[3] : 'ne',
                             allowed_ops[4] : 'eq',
                             allowed_ops[5] : 'gt',
                             allowed_ops[6] : 'lt',
                             allowed_ops[7] : 'like'}

                where_tokens = []
                identifier = None

                # TODO: Allow multiple WHERE predicates by converting this to a loop? 
                for item in tokenized.tokens:
                    if isinstance(item, Where):
                        for i in item.tokens:
                            if isinstance(i, Comparison):
                                where_tokens.append(str(i.left))
                                operation = str(i).split(" ")[1]
                                for op in allowed_ops:
                                    if op == operation:
                                        where_tokens.append(operator_strs[op])
                                        break
                                    if op.upper() == 'LIKE':
                                        where_tokens.append(operator_strs[op.upper()])
                                where_tokens.append(str(i.right))
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

            def format_ids(identifiers):
                if len(identifiers) == 0:
                    return ''
                formatted_ids = ', '.join(identifiers)
                return formatted_ids                

            '''
            Special case for CREATE INDEX or DESCRIBE TABLE
            '''
            if tokenized[0].ttype is DDL and tokenized[0].value.upper() == 'CREATE':
                sqlir = SQLIR().set_create_index(parse_create_index(tokenized))
                return sqlir
            elif tokenized[0].ttype is Keyword and tokenized[0].value.upper() == 'DESCRIBE':
                sqlir = SQLIR().set_describe_table(parse_describe_table(tokenized))
                return sqlir

            select_stream = parse_select_clause(tokenized)
            projection_ids = list(extract_identifiers(select_stream))

            from_stream = parse_from_clause(tokenized)
            table_ids = list(extract_identifiers(from_stream))

            selection_ids = parse_where_clause(tokenized)

            sqlir = SQLIR()

            sqlir.set_projection(format_ids(projection_ids))
            sqlir.set_selection(format_ids(selection_ids))
            sqlir.set_table_name(format_ids(table_ids))

            return sqlir


        sql_statement = sqlparse.split(raw_query)[0]
        
        tokenized = sqlparse.parse(sql_statement)[0]

        return parse_clauses(tokenized)

