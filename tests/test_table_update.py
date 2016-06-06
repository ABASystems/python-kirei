from unittest import TestCase
import numpy as np
from kirei import Table

from .fixtures import *


class TableUpdateTestCase(TestCase):
    """Test the update method on `Table`s.
    """
    def setUp(self):
        self.file = gen_linear_csv()
        self.table = Table(source=self.file.name, fields=FIELDS)
        self.other_file = gen_linear_csv(1000)
        self.other_table = Table(source=self.other_file.name, fields=FIELDS)
        self.empty_file = gen_empty_csv()
        self.empty_table = Table(source=self.empty_file.name, fields=FIELDS)

    def test_empty_left_and_right_tables(self):
        self.empty_table.update_from(self.empty_table, key='A')
        self.assertListEqual(
            list(self.empty_table.data.columns.values), ['A', 'B', 'C', 'D'],
            'column headers have been changed'
        )

    def test_update_all(self):
        self.table.update_from(self.other_table, key='A')
        self.assertListEqual(
            list(self.table.data['A']), list(map(str, range(10))),
            'key column has been changed'
        )
        self.assertListEqual(
            list(self.table.data['B']), list(self.other_table.data['B']),
            'B column not been changed'
        )
        self.assertListEqual(
            list(self.table.data['C']), list(self.other_table.data['C']),
            'C column not been changed'
        )
        self.assertListEqual(
            list(self.table.data['D']), list(self.other_table.data['D']),
            'D column not been changed'
        )
        self.assertListEqual(
            list(self.empty_table.data.columns.values), ['A', 'B', 'C', 'D'],
            'column headers have been changed'
        )

    def test_update_some(self):
        self.other_table.data.set_value(2, 'C', '')
        self.other_table.data.set_value(5, 'B', np.nan)
        self.table.update_from(self.other_table, key='A')
        self.assertListEqual(
            list(self.table.data['A']), list(map(str, range(10))),
            'key column has been changed'
        )
        val = list(self.other_table.data['B'])
        val[5] = self.table.data.at[5, 'B']
        self.assertListEqual(
            list(self.table.data['B']), val,
            'B column not been changed'
        )
        val = list(self.other_table.data['C'])
        val[2] = self.table.data.at[2, 'C']
        self.assertListEqual(
            list(self.table.data['C']), val,
            'B column not been changed'
        )
        self.assertListEqual(
            list(self.table.data['D']), list(self.other_table.data['D']),
            'B column not been changed'
        )
        self.assertListEqual(
            list(self.empty_table.data.columns.values), ['A', 'B', 'C', 'D'],
            'column headers have been changed'
        )

    def test_update_specific_columns(self):
        self.table.update_from(self.other_table, key='A', columns=('B', 'D'))
        self.assertListEqual(
            list(self.table.data['A']), list(map(str, range(10))),
            'key column has been changed'
        )
        self.assertListEqual(
            list(self.table.data['B']), list(self.other_table.data['B']),
            'B column not been changed'
        )
        self.assertListEqual(
            list(self.table.data['C']), list(map(str, [200 + v for v in range(10)])),
            'C column not been changed'
        )
        self.assertListEqual(
            list(self.table.data['D']), list(self.other_table.data['D']),
            'D column not been changed'
        )
        self.assertListEqual(
            list(self.empty_table.data.columns.values), ['A', 'B', 'C', 'D'],
            'column headers have been changed'
        )
