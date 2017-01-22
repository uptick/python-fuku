import argparse

from .db import get_default_db, save_db


class Client(object):
    def __init__(self):
        self.modules = []
        self.parser = argparse.ArgumentParser()
        self.db = get_default_db()

    def add_module(self, module):
        module = module(db=self.db, client=self)
        for mod in self.modules:
            if mod.name == module.name:
                raise TypeError('duplicate modules: {}'.format(mod.name))
        self.modules.append(module)

    def add_modules(self, *args):
        for mod in args:
            self.add_module(mod)

    def add_arguments(self):
        subp = self.parser.add_subparsers()
        for mod in self.modules:
            modp = subp.add_parser(mod.name)
            mod.add_arguments(modp)
            modp.set_defaults(handler=mod.entry)

    def get_module(self, name):
        for mod in self.modules:
            if mod.name == name:
                return mod
        raise KeyError('no module named {}'.format(name))

    def get_selected(self, module):
        return self.get_module(module).get_selected()

    def iter_parent_modules(self, name):
        for mod in self.modules:
            if name in mod.dependencies:
                yield mod

    def entry(self):
        self.add_arguments()
        self.args = self.parser.parse_args()
        try:
            handler = self.args.handler
        except AttributeError:
            handler = None
        if handler:
            handler(self.args)
        save_db(self.db)
