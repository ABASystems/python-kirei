import six
from decimal import Decimal, InvalidOperation

from dateutil.parser import parse as _parse_date
from django.core.exceptions import ValidationError


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
