import six
import math

import numpy as np
import pandas as pd


NA_VALUES = set([None, np.nan, pd.NaT])

EMPTY_VALUES = NA_VALUES | set([''])


def is_na(value):
    if value in NA_VALUES:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    return False


def is_empty(value):
    if value in EMPTY_VALUES:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    return False


def to_list(value):
    if value is None:
        return []
    if isinstance(value, six.string_types):
        return [value]
    return value


def to_set(value):
    return set(to_list(value))
