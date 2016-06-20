import re
import six
from decimal import Decimal, InvalidOperation
from datetime import timedelta
import logging

from dateutil.parser import parse as _parse_date
from django.core.exceptions import ValidationError
import numpy as np

from .utils import is_empty


logger = logging.getLogger(__name__)


DURATION_PROG = re.compile(r'((?P<days>\d*\.?\d+?)\s*(?:days|d))?((?P<hours>\d*\.?\d+?)\s*(?:hours|hrs?|h))?((?P<minutes>\d*\.?\d+?)\s*(?:minutes?|m))?((?P<seconds>\d*\.?\d+?)\s*(?:seconds|s))?', re.I)


def parse_date(date_str):
    if not isinstance(date_str, six.string_types):
        return date_str
    try:
        res = _parse_date(date_str)
    except AttributeError:
        res = None
    except ValueError:
        res = None
    if res is None:
        raise ValidationError('Failed to parse date "{}"'.format(date_str))
    return res


def parse_bool(value):
    if not isinstance(value, six.string_types):
        return bool(value)
    value = value.lower()
    if 'y' in value or '1' in value or 't' in value:
        return True
    elif 'n' in value or '0' in value or 'f' in value:
        return False
    raise ValidationError('Unable to parse boolean "{}"'.format(value))


def parse_decimal(value):
    if isinstance(value, six.string_types):
        value = value.strip()
        if not value:
            value = '0'
    try:
        return Decimal(value)
    except InvalidOperation:
        raise ValidationError('Unable to parse decimal "{}"'.format(value))


def parse_duration(duration_str, error='raise'):
    if is_empty(duration_str):
        return duration_str
    parts = DURATION_PROG.search(duration_str)
    if not parts:
        msg = 'Failed to parse duration: "{}".'.format(duration_str)
        if error == 'raise':
            raise Exception(msg)
        elif error == 'log':
            logger.error(msg)
        return np.nan
    parts = parts.groupdict()
    time_params = {}
    for (name, param) in parts.items():
        if param:
            time_params[name] = float(param)
    if not time_params:
        msg = 'Failed to parse duration: "{}".'.format(duration_str)
        if error == 'raise':
            raise Exception(msg)
        elif error == 'log':
            logger.error(msg)
        return np.nan
    return timedelta(**time_params)


def duration_parser(error='raise'):
    return lambda x: parse_duration(x, error)
