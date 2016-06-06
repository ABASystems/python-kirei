import os
import six
from collections import OrderedDict
import logging

import pandas as pd

from .utils import to_list


logger = logging.getLogger('kirei')

_all_table_cls = OrderedDict()
_all_tables = OrderedDict()
_ordered_tables = []


def register_tables(tables):
    for tbl in tables:
        register_table(tbl.__name__, tbl)


def register_table(name, cls):
    global _all_table_cls
    if name is None:
        name = cls.__name__
    assert name not in _all_table_cls, 'Duplicate tables: {}'.format(name)
    _all_table_cls[name] = cls
    cls.name = name


def get_table(name):
    global _all_tables
    if not isinstance(name, six.string_types):
        if isinstance(name, pd.DataFrame):
            return type('', (object,), {'data': name})
        return name
    return _all_tables.get(name, None)


def iter_tables():
    for table in _all_tables.values():
        yield table


def order_tables():
    done = set()
    for table in _all_tables.values():
        _order_tables(table, done)
    logger.info('Ordered tables {}'.format([t.name for t in _ordered_tables]))


def _order_tables(table, done):
    if table in done:
        return
    done.add(table)
    for jt in table.iter_dependencies():
        _order_tables(jt, done)
    _ordered_tables.append(table)


def run(in_root=None, out_root=None, include=[]):
    global _all_tables
    logger.info('Running kirei')
    if in_root is None:
        in_root = os.path.getcwd()
    for name, cls in _all_table_cls.items():
        if name not in _all_tables:
            table = cls(in_root, out_root=out_root, load=False)
            _all_tables[name] = table
    include = [_all_tables[i] for i in to_list(include)]
    if not include:
        include = list(_all_tables.values())
    for table in _all_tables.values():
        table.resolve_source()
    for table in include:
        table.process()
    # order_tables()

    # # Pass 1, load everything from files, clean data, filter.
    # for table in _ordered_tables:
    #     if table.source_type != 'file':
    #         continue
    #     table.load()
    #     table.clean_all_fields()
    #     table.filter()
    #     table.check_unique()

    # # Pass 2, 

    # for table in _ordered_tables:
    #     table.join_all_tables()
