import os
import sys
import json
import tempfile
from contextlib import contextmanager
from string import Template

from .runner import run


def merge_cfgs(a, b):
    y = {}
    for k, v in a.items():
        if k in b:
            y[k] = Template(v).safe_substitute({k: b[k]})
        else:
            y[k] = v
    for k, v in b.items():
        if k not in a:
            y[k] = v
    return y


def finish_merge_cfgs(a):
    x = a
    while 1:
        done = True
        y = {}
        for k, v in x.items():
            y[k] = Template(v).safe_substitute(x)
            if y[k] != v:
                done = False
        x = y
        if done:
            break
    return y


def subs(cmd, cfg):
    cmd = Template(cmd).substitute(cfg)
    return cmd


class Module(object):
    dependencies = []
    base_config = {
        'aws': 'aws'
    }

    def __init__(self, name, db=None, client=None):
        self.name = name
        self.db = db
        self.store = self.db.setdefault(self.name, {})
        self.client = client

    def add_arguments(self, parser):
        pass

    def config(self):
        return {}

    def get_module(self, name):
        return self.client.get_module(name)

    def get_selected(self):
        return None

    def error(self, msg):
        print(msg)
        sys.exit()

    def data_path(self, filename):
        path = os.path.join(
            os.path.dirname(
                os.path.realpath(__file__)
            ),
            'scripts',
            'data',
            filename
        )
        return path

    @contextmanager
    def template_file(self, filename, context={}):
        with tempfile.NamedTemporaryFile() as outf:
            with open(self.data_path(filename)) as inf:
                data = Template(inf.read()).substitute(context)
            outf.write(data.encode())
            outf.flush()
            yield outf.name

    def merged_config(self, cfg={}, use_self=False):
        if use_self:
            mods = [self]
        else:
            mods = [self.client.get_module(d) for d in self.dependencies]
        while len(mods):
            cur = mods.pop(0)
            cfg = merge_cfgs(cfg, cur.config())
            for d in cur.dependencies:
                mods.append(self.client.get_module(d))
        cfg = merge_cfgs(cfg, self.base_config)
        return finish_merge_cfgs(cfg)

    def run(self, cmd, cfg={}, capture='discard', use_self=False):
        cfg = self.merged_config(cfg, use_self)
        final = subs(cmd, cfg)
        # print(final)
        output = run(
            final,
            capture=capture not in set([None, '', False])
        )
        if capture == 'json':
            output = json.loads(output)
        return output

    def clear_parent_selections(self):
        for parent in self.client.iter_parent_modules(self.name):
            try:
                parent.select(type('opts', (object,), {'name': None, 'show': None}))
            except AttributeError:
                pass

    def entry(self, args):
        handler = getattr(args, '%s_handler' % self.name, None)
        if handler:
            handler(args)
