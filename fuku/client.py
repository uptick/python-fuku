import argparse
import logging

from colorama import Fore

from .db import get_default_db, save_db


class Client(object):
    global_arguments = {('app', 'application'), ('pg', 'DB instance')}

    def __init__(self):
        self.modules = []
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument(
            f'--log',
            help='Log level. Default: WARNING',
            choices=('CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'),
        )

        # global arguments
        for arg, verbose_name in self.global_arguments:
            self.parser.add_argument(
                f'--{arg}', metavar=arg.upper(),
                help=f'Global {verbose_name} argument overwriting context'
            )

        self.db = get_default_db()
        self.logger = logging.getLogger('fuku.client')

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
        self.logger.debug('Iterating parent modules')
        for mod in self.modules:
            if name in mod.dependencies:
                self.logger.debug(f'\t{mod.name} - dependencies: {mod.dependencies}')
                yield mod

    def iter_dependent_modules(self, parent):
        for mod in self.modules:
            if mod.name in parent.dependencies:
                yield mod

    def entry(self):
        self.add_arguments()
        self.args = self.parser.parse_args()

        # set the logging log level based on parsed arguments
        loglevel = vars(self.args).get('log') or 'WARNING'
        logformat = f'{Fore.CYAN}%(levelname)-10s {Fore.GREEN}%(name)s\t{Fore.RESET}%(message)s'
        logging.basicConfig(level=loglevel, format=logformat)

        try:
            handler = self.args.handler
        except AttributeError:
            handler = None
        if handler:
            handler(self.args)
        save_db(self.db)
