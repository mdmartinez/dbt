import unittest

import dbt.sql


class TestSqlManipulations(unittest.TestCase):

    def test__hoist_ctes(self):
        start = 'with a as (with b as (select * from s.b) select * from b) select * from a'  # noqa
        expected = 'with b as (select * from s.b), a as (select * from b) select * from a'  # noqa

        self.assertEqual(dbt.sql.hoist_ctes(start), expected)
