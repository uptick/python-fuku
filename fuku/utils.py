import argparse
import string
import random


class StoreKeyValuePair(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None, sep='='):
        if not isinstance(values, list):
            values = [values]
        for pair in values:
            k, v = pair.split(sep)
        setattr(namespace, self.dest, dict([(k, v) for k, v in [p.split(sep) for p in values]]))


class StorePortPair(StoreKeyValuePair):
    def __call__(self, *args, **kwargs):
        super().__call__(*args, sep=':', **kwargs)


def env_to_dict(env):
    res = {}
    for pair in env:
        res[pair['name']] = pair['value']
    return res


def dict_to_env(val):
    env = []
    for k, v in val.items():
        env.append({'name': k, 'value': v})
    return env


def env_to_string(env, opt='-e'):
    if env:
        return ' ' + ' '.join([
            '%s %s=%s' % (opt, k, v) for k, v in env_to_dict(env).items()
        ])
    else:
        return ''


def ports_to_dict(env):
    res = {}
    for pair in env:
        res[pair['hostPort']] = pair['containerPort']
    return res


def dict_to_ports(val):
    env = []
    for k, v in val.items():
        env.append({'hostPort': int(k), 'containerPort': int(v), 'protocol': 'tcp'})
    return env


def ports_to_string(val, opt='-p'):
    if val:
        return ' ' + ' '.join([
            '%s %s:%s' % (opt, k, v) for k, v in ports_to_dict(val).items()
        ])
    else:
        return ''


def gen_secret(length=64):
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(length))
