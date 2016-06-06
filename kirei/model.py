import six
import logging

from django.apps import apps

from .config import get_config
from .utils import *


logger = logging.getLogger(__name__)


def coerce(value):
    """Convert usual suspects into things we can use.
    """
    if is_na(value):
        return None
    return value


def get_model(model):
    if isinstance(model, six.string_types):
        return apps.get_model(model)
    return model


def handle_fk(row, values, info):
    """Try and get value from `values` first, as this will contain other
    FKs.
    """
    model = get_model(info['model'])
    kwargs = {}
    for src, dst in info['fields'].items():
        if src in values:
            kwargs[dst] = values[src]
        elif src in row and not is_na(row[src]):
            kwargs[dst] = row[src]
    # If we end up with no fields to check for don't continue.
    if not kwargs:
        return None
    #
    try:
        return model.objects.get(**kwargs)
    except (model.DoesNotExist, model.MultipleObjectsReturned) as err:
        err.args = (err.args[0] + ': {}'.format(kwargs),)
        raise err


def build_values(values, names):
    return dict([(c, values[c]) for c in names if c in values])


def resolve_callable(value, table):
    if isinstance(value, six.string_types):
        return getattr(table, value)
    return value


def save_instance(table, model, row, **kwargs):
    unique = to_list(kwargs.get('unique', []))
    exclude = to_set(kwargs.get('exclude', []))
    include = to_set(kwargs.get('include', []))
    if len(include) == 0:
        include |= set(row.axes[0].values)
    include -= exclude
    values = dict([(v, row[v]) for v in row.axes[0].values if not is_na(row[v])])
    for name, info in kwargs.get('fields', {}).items():
        val = handle_fk(row, values, info)
        if val is not None:
            values[name] = val
    if unique:
        creator = resolve_callable(kwargs.get('get_or_creator', model.objects.update_or_create), table)
        unique_values = []
        for name in unique:
            try:
                unique_values.append(values[name])
            except KeyError:
                unique_values.append(None)
                logger.warning(
                    ('A model field, "{}", was specified as a unique value, but there is no '
                     'available value.'.format(name))
                )
        kwargs = dict(zip(unique, unique_values))
        kwargs['defaults'] = build_values(values, (include - set(unique)))
        inst, created = model.objects.update_or_create(**kwargs)
    else:
        creator = resolve_callable(kwargs.get('creator', model.objects.create), table)
        inst = creator(**build_values(values, include))
        created = True
    return inst, created


def save_model(table, df, model_name, info):
    if not get_config('models', True) and not getattr(table, 'force_model_output', False):
        return
    model = get_model(model_name)
    for ii, row in df.iterrows():
        inst, created = save_instance(table, model, row, **info)
        post = getattr(table, 'post_save_model', None)
        if post:
            post(row, inst, created)
        msg = '{} {} instance: {}'.format(
            'Created' if created else 'Updated',
            model.__name__,
            inst
        )
        logger.info(msg)
