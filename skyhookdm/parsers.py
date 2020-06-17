import re


# ------------------------------
# Query Parsers

class QueryParser(object):

    query_structure_template = r'SELECT\s+(.*)\s+FROM\s+(.*)'

    group_ndx_select = 1
    group_ndx_from   = 2

    @classmethod
    def parse_clause_select(cls, select_clause):
        projection_attrs = [
            projection_attr.strip()
            for projection_attr in select_clause.strip().split(',')
        ]

        if any(projection_attr == '*' for projection_attr in projection_attrs):
            return []

        return projection_attrs

    @classmethod
    def parse_clause_from(cls, from_clause):
        return [
            relation.strip()
            for relation in from_clause.strip().split(',')
        ]

    @classmethod
    def parse_clause_where(cls, where_clause):
        pass

    @classmethod
    def parse(cls, query_string):
        matched_query = re.search(cls.query_structure_template, query_string)

        projection_attrs = cls.parse_clause_select(matched_query.group(cls.group_ndx_select))
        query_relations  = cls.parse_clause_from(matched_query.group(cls.group_ndx_from))

        return {
            'projection': projection_attrs,
            'relations' : query_relations,
            'predicates': '',
            'aggregates': '',
        }
