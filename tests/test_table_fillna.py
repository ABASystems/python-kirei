from unittest import TestCase

import numpy as np
from kirei import Table

from .fixtures import *


class TableFillnaTestCase(TestCase):
    """Test the fillna_from method on `Table`s.
    """
    def setUp(self):
        self.file = gen_linear_csv()
        self.table = Table(source=self.file.name, fields=FIELDS)
        self.other_file = gen_linear_csv(1000)
        self.other_table = Table(source=self.other_file.name, fields=FIELDS)
        self.empty_file = gen_empty_csv()
        self.empty_table = Table(source=self.empty_file.name, fields=FIELDS)

    def test_fill_some(self):
        orig = self.table.data.copy()
        self.table.data.set_value(2, 'C', np.nan)
        self.table.data.set_value(5, 'B', np.nan)
        self.table.fillna_from(self.other_table, key='A')
        self.assertListEqual(
            list(self.table.data['A']), list(map(str, range(10))),
            'key column has been changed'
        )
        val = list(orig['B'])
        val[5] = self.other_table.data.at[5, 'B']
        self.assertListEqual(
            list(self.table.data['B']), val,
            'B column not been changed'
        )
        val = list(orig['C'])
        val[2] = self.other_table.data.at[2, 'C']
        self.assertListEqual(
            list(self.table.data['C']), val,
            'B column not been changed'
        )
        self.assertListEqual(
            list(self.table.data['D']), list(orig['D']),
            'B column not been changed'
        )
        self.assertListEqual(
            list(self.empty_table.data.columns.values), ['A', 'B', 'C', 'D'],
            'column headers have been changed'
        )
