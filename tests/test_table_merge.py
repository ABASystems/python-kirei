from unittest import TestCase
import numpy as np
from kirei import Table

from .fixtures import *


class TableJoinTableTestCase(TestCase):
    """Test the `join_table` method on `Table`s.
    """
    def setUp(self):
        self.file = gen_linear_csv()
        self.table = Table(source=self.file.name, fields=FIELDS)
        self.other_file = gen_linear_csv(1000)
        self.other_table = Table(source=self.other_file.name, fields=FIELDS)
        self.other_table.rename_field('D', 'E')

    def test_field_rename_one(self):
        self.table.join_table(self.other_table, {
            'on': 'A',
            'how': 'left',
            'fields': {
                'B': {
                    'rename': 'F'
                }
            }
        })
        self.assertListEqual(
            list(self.table.data['A']), list(map(str, range(10))),
            'key column has been changed'
        )
        self.assertListEqual(
            list(self.table.data['B']), list(map(str, range(100, 110))),
            'B column should be left values'
        )
        self.assertListEqual(
            list(self.table.data['C']), list(map(str, range(200, 210))),
            'C column should be left values'
        )
        self.assertListEqual(
            list(self.table.data['D']), list(map(str, range(300, 310))),
            'D column should be left values'
        )
        self.assertListEqual(
            list(self.table.data['E']), list(map(str, range(1300, 1310))),
            'E column should be right values'
        )
        self.assertListEqual(
            list(self.table.data['F']), list(map(str, range(1100, 1110))),
            'F column should be right values'
        )

    def test_field_rename_two(self):
        self.table.join_table(self.other_table, {
            'on': 'A',
            'how': 'left',
            'fields': {
                'B': {
                    'rename': 'F'
                },
                'C': {
                    'rename': 'G'
                },
            }
        })
        self.assertListEqual(
            list(self.table.data['A']), list(map(str, range(10))),
            'key column has been changed'
        )
        self.assertListEqual(
            list(self.table.data['B']), list(map(str, range(100, 110))),
            'B column should be left values'
        )
        self.assertListEqual(
            list(self.table.data['C']), list(map(str, range(200, 210))),
            'C column should be left values'
        )
        self.assertListEqual(
            list(self.table.data['D']), list(map(str, range(300, 310))),
            'D column should be left values'
        )
        self.assertListEqual(
            list(self.table.data['E']), list(map(str, range(1300, 1310))),
            'E column should be right values'
        )
        self.assertListEqual(
            list(self.table.data['F']), list(map(str, range(1100, 1110))),
            'F column should be right values'
        )
        self.assertListEqual(
            list(self.table.data['G']), list(map(str, range(1200, 1210))),
            'G column should be left values'
        )

    def test_field_needs_rename(self):
        """By placing 'B' in the 'fields' entry we cause an exception
        due to the field B already existing in the left table.
        """
        with self.assertRaises(Exception):
            self.table.join_table(self.other_table, {
                'on': 'A',
                'how': 'left',
                'fields': {
                    'B': {}
                }
            })
