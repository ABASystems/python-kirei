_config = {
}


def set_config(value, state):
    _config[value] = state


def get_config(value, default=None):
    return _config.get(value, default)
