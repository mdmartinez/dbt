import re
import sqlparse


class TokenType:
    DML = 'Token.Keyword.DML'

    @classmethod
    def match(cls, token, _type):
        return isinstance(token, sqlparse.sql.Token) and \
            str(token.ttype) == _type

    @classmethod
    def is_identifier(cls, token):
        return isinstance(token, sqlparse.sql.Identifier)

    @classmethod
    def is_dml(cls, token):
        return cls.match(token, cls.DML)

    @classmethod
    def is_identifier_list(cls, token):
        return isinstance(token, sqlparse.sql.IdentifierList)


def parse_sql(sql):
    result = sqlparse.parse(sql)
    return result[0]


CTE_REGEX = re.compile('(?:with\s+)?(?P<name>\S*)\s+as\s+\((?P<sql>.*)\)', re.S)  # noqa


def parse_cte_sql(sql):
    result = CTE_REGEX.search(sql)

    if result is None:
        return None

    return result.groups()


def get_ctes(sql):
    statement = parse_sql(sql)

    # ctes are tuples of (identifier, dml)
    result = {'ctes': [], 'dml': None}
    in_cte = False

    # split the sql into (pre, dml)
    #   pre == ctes
    #   dml == select * from ...
    # iterate through cte(s) and convert to tuple representation (identifier, dml)
    #   recurse
    # intermediate query representation
    #   { 'ctes': (('identifier', 'dml',), ...), 'dml': '...' }
    # join the query back together

    dml_index = None

    for index, token in enumerate(statement):
        if TokenType.is_dml(token):
            dml_index = index
            break

    if dml_index is None:
        return {'ctes': [], 'dml': sql}

    ctes = statement[0:dml_index]

    dml = statement[dml_index:]
    result['dml'] = ''.join([d.value for d in dml])

    for token in ctes:
        if TokenType.is_identifier(token):
            groups = parse_cte_sql(token.value)
            if groups is None:
                continue
            identifier, dml = groups
            parsed = get_ctes(dml)

            result['ctes'] = result['ctes'] + parsed['ctes']
            result['ctes'].append((identifier, parsed['dml'],))

        elif TokenType.is_identifier_list(token):
            for identifier_token in token.get_identifiers():
                identifier, dml = parse_cte_sql(identifier_token.value)
                parsed = get_ctes(dml)

                result['ctes'] += parsed['ctes']
                result['ctes'] += (identifier, parsed['dml'])

    return result


def hoist_ctes(sql):
    result = get_ctes(sql)

    return 'with {} {}'.format(
        ', '.join(['{} as ({})'.format(identifier, dml)
                   for (identifier, dml) in result['ctes']]),
        result['dml'])
