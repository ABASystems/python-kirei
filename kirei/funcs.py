import six

import numpy as np


def clean_string(value):
    if not isinstance(value, six.string_types):
        return value
    value = value.replace(u'\xa0', u' ').strip()
    if not value:
        return np.nan
    return value
