import os
import six
import csv
import functools
import logging
from datetime import date
import tempfile

import numpy as np
import pandas as pd

from .registry import register_table, get_table
from .model import save_model
from .funcs import clean_string
from .utils import to_list


logger = logging.getLogger('kirei.table')


def run_once(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        attr = '_run_once_' + f.__name__
        if getattr(args[0], attr, False):
            return
        setattr(args[0], attr, True)
        f(*args, **kwargs)
    return wrapper


# class TableMetaclass(type):

#     def __new__(cls, name, bases, attrs):
#         final_cls = super(TableMetaclass, cls).__new__(cls, name, bases, attrs)
#         if name not in ('TableMetaclass', 'Table'):
#             register_table(name, final_cls)
#         return final_cls


# @six.add_metaclass(TableMetaclass)
class Table(object):
    source = None
    fields = {}
    joins = {}

    def __init__(self, root=None, source=None, fields=None, load=True, out_root=None):
        if not hasattr(self, 'name'):
            self.name = self.__class__.__name__
        self.root = root
        self.out_root = out_root
        if source is not None:
            self.source = source
        if fields is not None:
            self.fields = fields
        if load:
            self.load()

    def resolve_source(self):
        if isinstance(self.source, six.string_types):

            # Prefix with a specified root value, if given.
            if self.root is not None:
                path = os.path.join(self.root, self.source)
            else:
                path = self.source

            # Check for table/path conflicts.
            tbl = get_table(self.source)
            if os.path.exists(path) and tbl is not None:
                raise Exception('Aliased source file and table: "{}"'.format(self.source))

            if tbl is not None:
                self.source_type = 'table'
            else:
                self.source_type = 'file'

        # File-like object is assumed.
        else:
            self.source_type = 'file'

        logger.info('Resolved {} to {}'.format(self.name, self.source_type))

    @run_once
    def process(self):
        logger.info('Processing {}'.format(self.name))
        if self.source_type == 'table':
            get_table(self.source).process()
        for dep in self.iter_join_tables():
            dep.process()
        self.load()
        self.clean_all_fields()
        self.filter()
        self.check_unique()
        self.join_all_tables()
        self.do_validate()
        self.save()
        logger.info('Done processing {}'.format(self.name))

    @run_once
    def load(self):
        if not hasattr(self, 'source_type'):
            self.resolve_source()
        if isinstance(self.source, six.string_types):

            # Prefix with a specified root value, if given.
            if self.root is not None:
                path = os.path.join(self.root, self.source)
            else:
                path = self.source

            # Check for table/path conflicts.
            tbl = get_table(self.source)
            if os.path.exists(path) and tbl is not None:
                raise Exception('Aliased source file and table: "{}"'.format(self.source))

        # File-like object is assumed.
        else:
            path = self.source

        if tbl is not None:
            df = tbl.data
            if getattr(self, 'fields', None):
                df = df[list(self.fields.keys())]
            self.data = df.copy()
        else:
            extra = {}
            if getattr(self, 'fields', None):

                # Produce a mapping of types.
                dtypes = {}
                parse_dates = []
                for name, info in self.fields.items():
                    type = info.get('type', None)
                    if type == date:
                        parse_dates.append(name)
                    if type is not None:
                        dtypes[name] = type
                if dtypes:
                    extra['dtype'] = dtypes
                if parse_dates:
                    extra['parse_dates'] = parse_dates

                # Maybe use all fields.
                extra['usecols'] = self.fields.keys()

            self.data = pd.read_csv(path, header=0, **extra)

        # If we have no fields then populate a list.
        if not getattr(self, 'fields', None):
            self.fields = dict([(f, {}) for f in self.data.columns])

    @classmethod
    def get(cls):
        return get_table(cls.name)

    @run_once
    def clean_all_fields(self):
        if hasattr(self, 'fields'):
            self.data = self.data[list(self.fields.keys())]
        for name, info in self.fields.items():
            func = getattr(self, 'clean_' + name, self.clean_field)
            func(name, info)

    def clean_field(self, name, info):
        if 'type' in info:
            # Cache the nans for insertion after coercion.
            nans = self.data[name].isnull()
            new_col = self.data[name].astype(info['type'])
            self.data.ix[nans, name] = np.nan
            # Catch any bogus conversions.
            if nans.sum() != new_col.isnull().sum():
                lost = self.data[name][new_col.isnull()]
                lost = lost[lost.notnull()]
                msg = 'During conversion of "{}" for table "{}" NAs were introduced.'.format(
                    name, self.name,
                )
                raise Exception(msg)
            # Set the final value.
            self.data[name] = new_col
        logger.info('Cleaning field {}."{}"'.format(self.name, name))
        self.data[name] = self.data[name].map(clean_string)
        if 'rename' in info:
            self.rename_field(name, info['rename'])

    @run_once
    def filter(self):
        pass

    def rename_field(self, name, new_name):
        logger.info('Renaming field {0}."{1}" to {0}."{2}"'.format(
            self.name, name, new_name
        ))
        self.data.rename(columns={name: new_name}, inplace=True)

    def drop_field(self, name):
        self.data.drop(name, axis=1, inplace=True)

    def drop_empty(self, name):
        # self.data = self.data[self.data.apply(lambda x: not is_empty(x[name]), axis=1)]
        self.data = self.data[self.data[name].notnull()]
        self.data = self.data[self.data.apply(lambda x: x[name] != '', axis=1)]

    def has_duplicates(self, *fields):
        dups = self.data.duplicated(subset=fields)
        dups = dups[dups == True]
        return not dups.empty

    def check_unique(self):
        for fields in getattr(self, 'unique', []):
            if isinstance(fields, six.string_types):
                fields = [fields]
            assert not self.has_duplicates(*fields), '{} has duplicate values in field(s) {}'.format(self.name, fields)

    @run_once
    def join_all_tables(self):
        for name, info in self.joins.items():
            func = getattr(self, 'join_' + name, self.join_table)
            func(name, info)

    def join_table(self, name, info, inplace=True):
        if not info:
            return
        right = get_table(name)
        cols_to_use, join_cols, rename_cols = self._get_cols_to_use(right, info)
        logger.info('Joining tables {} and {} on "{}"'.format(
            self.name, name, join_cols
        ))            
        result = self.data.merge(right.data[cols_to_use], **info)
        if rename_cols:
            result.rename(columns=rename_cols, inplace=True)
        for col in join_cols:
            for suf in ('_x', '_y'):
                name = col + suf
                if name in result:
                    result.drop(name, axis=1, inplace=True)
        if inplace:
            self.data = result
        return result

    def _get_cols_to_use(self, right, info):
        cols_to_use = right.data.columns.difference(self.data.columns)
        if 'on' in info:
            join_cols = info['on']
            if not isinstance(join_cols, (list, tuple)):
                join_cols = [join_cols]
            cols_to_use = cols_to_use.append(pd.Series(join_cols))
        else:
            join_cols = []
            if 'left_on' in info:
                if isinstance(info['left_on'], (list, tuple)):
                    join_cols.extend(info['left_on'])
                else:
                    join_cols.append(info['left_on'])
            if 'right_on' in info:
                if isinstance(info['right_on'], (list, tuple)):
                    join_cols.extend(info['right_on'])
                    cols_to_use = cols_to_use.append(pd.Series(info['right_on']))
                else:
                    join_cols.append(info['right_on'])
                    cols_to_use = cols_to_use.append(pd.Series([info['right_on']]))
        fields = info.pop('fields', None)
        cols_to_rename = {}
        if fields is not None:
            cols_to_use = cols_to_use.append(pd.Series(list(fields.keys())))
            for name, field_info in fields.items():
                if name in self.data:
                    if 'rename' not in field_info:
                        raise Exception('Trying to join with overlapping fields')
                    cols_to_rename[name + '_y'] = field_info['rename']
                    cols_to_rename[name + '_x'] = name
                else:
                    cols_to_rename[name] = field_info['rename']
        return cols_to_use.unique(), join_cols, cols_to_rename

    @run_once
    def do_validate(self):
        if hasattr(self, 'validate'):
            self.validate(self.data)

    @classmethod
    def iter_join_tables(self):
        for table in getattr(self, 'joins', {}).keys():
            yield get_table(table)

    def join_columns(self, *args, joiner=', '):
        df = self.data
        return df.apply(lambda x: joiner.join([str(x[c]) for c in args if x[c] and not pd.isnull(x[c])]), axis=1)

    def condense(self, other, key, field):
        """Join a set of values using groups.
        """
        key = to_list(key)
        tbl = get_table(other)
        tbl.process()
        df = tbl.data
        grp = df.groupby(key)[field].apply(
            lambda x: ','.join(x.sort_values())
        )
        return self.data[key].apply(
            lambda x: grp.get(tuple([x[k] for k in key]), np.nan),
            axis=1
        )

    def update_from(self, other, key, columns=None):
        key = to_list(key)
        other = get_table(other)
        if columns is None:
            columns = other.data.columns.values
        columns = to_list(columns)
        columns = list(set(columns) - set(key))
        da = self.data
        db = other.data
        values = da[key].merge(db[columns + key], on=key, how='left')
        values.index = da.index  # SO IMPORTANT
        da.update(values[columns])

    def fillna_from(self, other, key, columns=None):
        key = to_list(key)
        other = get_table(other)
        if columns is None:
            columns = other.data.columns.values
        columns = to_list(columns)
        columns = list(set(columns) - set(key))
        da = self.data
        db = other.data
        values = da[key].merge(db[columns + key], on=key, how='left')
        values.index = da.index  # SO IMPORTANT
        da.fillna(values[columns], inplace=True)

    def save(self, path=None, **kwargs):
        if not path:
            path = self.out_root
            if path and path[-1] != '/':
                path += '/'
        if not path:
            path = './'
        if path[-1] == '/' and not kwargs:
            base = path
            if not hasattr(self, 'output'):
                return
            output = self.output.items()
        else:
            base = None
            output = [(path, kwargs)]
        for path, info in output:

            # Check for a Django model.
            if info.get('type', None) == 'model':
                self.save_model(path, info)
                continue

            if base:
                path = os.path.join(base, path)
            logger.info('Saving {} to {}'.format(self.name, path))
            df = self.data

            # If unique has been given then apply it now.
            unique = info.pop('unique', None)
            if unique is not None:
                df = df.drop_duplicates(unique)

            if 'fields' in info:
                info['columns'] = info.pop('fields')
            df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL, **info)

    def save_model(self, model_name, info):
        df = self.data

        # TODO: Can't do this due to FK fields.
        # # If unique has been given then apply it now.
        # unique = info.get('unique', None)
        # if unique is not None:
        #     df = df.drop_duplicates(unique)

        save_model(self, df, model_name, info)

    def save_temporary(self, df=None, path=None, append=False, **kwargs):
        if path is None:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', dir='.') as tf:
                path = tf.name
        if df is None:
            df = self.data
        filename = os.path.split(path)[1]
        if append:
            path = open(path, 'a')
        df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL, **kwargs)
        return filename

    @classmethod
    def autofill(cls, key, columns, lower=True, group_key='__group_key__'):
        logger.info('Autofilling values using key {}'.format(key))
        if isinstance(columns, six.string_types):
            columns = [columns]
        dd = cls.get().data
        if lower:
            dd[group_key] = dd[key].map(str.lower)
        dd.replace('', np.nan, inplace=True)
        gr = dd.groupby(group_key)
        for name, group in gr:
            for col in columns:
                if group[col].count() and group[col].isnull().sum():
                    values = group[col][group[col].notnull()]
                    if len(values.unique()) == 1:
                        logger.info('Filling {} with {} value(s) - {}'.format(
                            col, len(group[col][group[col].isnull()]), name
                        ))
                        dd[col][dd[group_key] == name] = values.iloc[0]
                    elif len(values.unique()) > 1:
                        logger.warning('Inconsistent values for {}: {}'.format(
                            name, values.unique()
                        ))
