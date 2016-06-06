import inspect

from .table import Table
from .config import set_config, get_config
from .registry import (
    run, iter_tables, register_tables
)


def register_global_tables(data):
    tables = [o for o in data.values() if inspect.isclass(o) and issubclass(o, Table) and o is not Table]
    register_tables(tables)


def save_all(base=None):
    for table in iter_tables():
        table.save(base)
