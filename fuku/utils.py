import argparse
import random
import string
from contextlib import contextmanager
from datetime import datetime

import botocore


class EntityAlreadyExists(Exception):
    pass


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
            '%s %s="%s"' % (opt, k, v) for k, v in env_to_dict(env).items()
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


def volumes_to_dict(volumes):
    res = {}
    for info in volumes:
        res[info['name']] = info.get('host', {}).get('sourcePath', None)
    return res


def dict_to_volumes(val):
    volumes = []
    for k, v in val.items():
        item = {'name': k}
        if v:
            item['host'] = {'sourcePath': v}
        volumes.append(item)
    return volumes


def mounts_to_dict(mounts):
    res = {}
    for info in mounts:
        res[info['sourceVolume']] = {
            'containerPath': info['containerPath'],
            'readOnly': info['readOnly']
        }
    return res


def dict_to_mounts(val):
    mounts = []
    for k, v in val.items():
        mounts.append({
            'sourceVolume': k,
            'containerPath': v['containerPath'],
            'readOnly': v.get('readOnly', False)
        })
    return mounts


def mounts_to_string(val, existing={}, opt='--mount'):
    if val:
        mounts = mounts_to_dict(val)
        elems = []
        for src, dst in mounts.items():
            if src not in existing:
                elems.append('%s src=%s,dst=%s' % (opt, src, dst))
        for src, dst in existing.items():
            if src not in mounts.keys():
                elems.append('--mount-rm %s' % dst)
        return ' ' + ' '.join(elems)
    else:
        return ''


def gen_secret(length=64):
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(length))


def gen_name(length=16):
    return ''.join(
        random.SystemRandom().choice(string.ascii_uppercase) +
        random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(length - 1)
    )


@contextmanager
def entity_already_exists(hide=True):
    try:
        yield
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] not in [
                'EntityAlreadyExists',
                'InvalidKeyPair.Duplicate',
                'InvalidGroup.Duplicate',
                'InvalidPermission.Duplicate',
                'RepositoryAlreadyExistsException',
                'ResourceAlreadyExistsException'
        ]:
            raise
        if not hide:
            raise EntityAlreadyExists


@contextmanager
def limit_exceeded():
    try:
        yield
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'LimitExceeded':
            raise


def json_serial(obj):
    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError('type not serializable')
