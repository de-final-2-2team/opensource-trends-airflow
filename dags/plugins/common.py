from functools import reduce


def deep_get(dictionary:dict, keys:str, default=None):
    return reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."), dictionary)