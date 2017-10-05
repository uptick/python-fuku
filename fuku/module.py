import json
import logging
import os
import stat
import sys
import tempfile
import unicodedata
from contextlib import contextmanager
from string import Template

import boto3

from .db import get_rc_path
from .runner import run


class Module(object):
    dependencies = []

    def __init__(self, name, db=None, client=None):
        self.name = name
        self.db = db
        self.store = self.db.setdefault(self.name, {})
        self.client = client
        self.use_context = True
        self._checks = {}

    def add_arguments(self, parser):
        pass

    def validate(self, name):
        for char in {'-', '_', ' ', '/'}:
            if char in name:
                self.error(f'Invalid identifier in name: {unicodedata.name(char)}')

        if name == 'fuku':
            self.error('"fuku" is a reserved name')

    def get_context(self, ctx={}, use_context=True):
        for dep in self.client.iter_dependent_modules(self):
            ctx = dep.get_context(ctx)
        if self.use_context and use_context:
            ctx.update(self.get_my_context())
        return ctx

    def get_module(self, name):
        return self.client.get_module(name)

    def get_selected(self):
        return None

    def error(self, msg):
        print(msg)
        sys.exit()

    def register_check(self, key, call):
        self._checks[key] = call

    def data_path(self, filename=None):
        path = os.path.join(
            os.path.dirname(
                os.path.realpath(__file__)
            ),
            'scripts',
            'data'
        )
        if filename:
            path = os.path.join(path, filename)
        return path

    @contextmanager
    def template_file(self, filename, context={}):
        # with tempfile.NamedTemporaryFile() as outf:
        with open(self.data_path(filename)) as inf:
            data = Template(inf.read()).substitute(context)
        # outf.write(data.encode())
        # outf.flush()
        # yield outf.name
        yield data

    @contextmanager
    def temporary_file(self):
        tf = tempfile.NamedTemporaryFile()
        try:
            yield tf
        finally:
            tf.close()

    def run(self, cmd, cfg={}, capture='discard', use_self=False, ignore_errors=False,
            env={}):
        # cfg = self.merged_config(cfg, use_self)
        # final = subs(cmd, cfg)
        # print(final)
        env_copy = os.environ.copy()
        env_copy.update(env)
        output = run(
            # final,
            cmd,
            capture=capture not in set([None, '', False]),
            ignore_errors=ignore_errors,
            env=env_copy
        )
        if capture == 'json':
            output = json.loads(output)
        return output

    def clear_parent_selections(self):
        self.get_logger().debug(f'Clearing parent selections for {self.name}')
        for parent in self.client.iter_parent_modules(self.name):
            try:
                parent.select(None)
            except AttributeError:
                pass

    def confirm_remove(self, id):
        """ I want to remove something, confirm with parents that this
        is okay.
        """
        results = []
        for parent in self.client.iter_parent_modules(self.name):
            results.extend(parent.dependency_removal(self, id))
        if len(results):
            print('Unable to remove as there are dependencies:\n')
            for res in results:
                mod = res['module'].name
                id = res['id']
                print(f'  module "{mod}", identifier "{id}"')
            print('')

    def dependency_removal(self, module, id):
        """ Something higher up in the tree is going to be removed, check what that
        means for lower modules.
        """
        return []

    def check(self, key):
        if key in self._checks:
            return self._checks[key]()
        else:
            for parent in self.client.iter_parent_modules(self.name):
                res = parent.check(key)
                if res is not None:
                    return res

    def entry(self, args):
        handler = getattr(args, '%s_handler' % self.name, None)
        if handler:
            handler(args)

    def save(self):
        return self.store

    def load(self, cache):
        if 'selected' in cache:
            self.store['selected'] = cache['selected']
        else:
            try:
                del self.store['selected']
            except KeyError:
                pass

    def store_set(self, key, value):
        self.get_logger().debug(f'Set {self.name} store {self.store} with {key}={value}')

        if value:
            self.store[key] = value
            if key == 'selected':
                self.get_logger().info(f'selected: {value}')
        else:
            try:
                del self.store[key]
            except KeyError:
                pass

    def store_get(self, key):
        parts = key.split('.')
        value = self.store
        for p in parts:
            if value is not None:
                value = value.get(p, None)
        return value

    def db_get(self, key):
        parts = key.split('.')
        value = self.store
        for p in parts:
            if value is not None:
                value = value.get(p, None)
        return value

    def setup_boto_session(self, ctx={}):
        ctx = self.get_context(ctx, use_context=False)
        kwargs = {}
        if 'region' in ctx:
            kwargs['region_name'] = ctx['region']
        if 'profile' in ctx:
            kwargs['profile_name'] = ctx['profile']
        boto3.setup_default_session(**kwargs)

    def get_boto_resource(self, resource, ctx={}):
        self.setup_boto_session(ctx)
        return boto3.resource(resource)

    def get_boto_client(self, resource, ctx={}):
        self.setup_boto_session(ctx)
        return boto3.client(resource)

    def puts3(self, key, value):
        ctx = self.get_context()
        s3 = self.get_boto_client('s3')
        s3.put_object(
            Bucket=ctx['bucket'],
            Key=f'fuku/{key}',
            Body=json.dumps(value)
        )

    def gets3(self, key):
        try:
            ctx = self.get_context(use_context=False)
            s3 = self.get_boto_client('s3')
            data = s3.get_object(
                Bucket=ctx['bucket'],
                Key=f'fuku/{key}',
            )['Body'].read()
            return data
        except:
            return None

    def iters3(self, prefix):
        try:
            ctx = self.get_context(use_context=False)
            s3 = self.get_boto_resource('s3')
            bucket = s3.Bucket(ctx['bucket'])
            for obj in bucket.objects.filter(Prefix=prefix):
                yield obj
        except:
            pass

    def encrypt_file(self, path, purpose='an unknown resource'):
        print(f'\nPlease enter a password to secure {purpose}.')
        print('Be sure to keep this safe, as it is required for future')
        print('access to this resource.\n')
        while 1:
            try:
                self.run('gpg -c {}'.format(path))
                break
            except:
                print('\ntimed out, please try again\n')

    def get_secure_file(self, path):
        full_path = os.path.join(get_rc_path(), path)
        if not os.path.exists(full_path):
            data = self.gets3(f'{path}.gpg')
            if data is None:
                self.error(f'no secure key file found: {path}')
            try:
                os.makedirs(os.path.dirname(full_path))
            except OSError:
                pass
            with open(f'{full_path}.gpg', 'wb') as file:
                file.write(data)
            try:
                self.run(f'gpg -d {full_path}.gpg > {full_path}')
            except:
                self.clear_secure_file(path)
                raise
            os.chmod(full_path, stat.S_IRUSR | stat.S_IWUSR)
        return full_path

    def clear_secure_file(self, path):
        full_path = os.path.join(get_rc_path(), path)
        try:
            os.remove(full_path)
        except:
            pass

    def get_my_context(self):
        return {}

    def escape(self, value):
        return value.replace('"', '\\"')

    def get_logger(self):
        logger = logging.getLogger(f'fuku.{self.name}')
        return logger
