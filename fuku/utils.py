import argparse


class StoreKeyValuePair(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if not isinstance(values, list):
            values = [values]
        for pair in values:
            k, v = pair.split('=')
        setattr(namespace, self.dest, dict([(k, v) for k, v in [p.split('=') for p in values]]))


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
