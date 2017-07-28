import pprint

from .module import Module


class Session(Module):
    def __init__(self, **kwargs):
        super().__init__('session', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='session help')

        p = subp.add_parser('sv', help='save session')
        p.add_argument('name', help='session name')
        p.set_defaults(session_handler=self.handle_save)

        p = subp.add_parser('ld', help='load session')
        p.add_argument('name', help='session name')
        p.set_defaults(session_handler=self.handle_load)

        p = subp.add_parser('ls', help='list sessions')
        p.set_defaults(session_handler=self.handle_list)

        p = subp.add_parser('sh', help='show current state')
        p.set_defaults(session_handler=self.handle_show)

    def handle_save(self, args):
        cache = {}
        for mod in self.client.modules:
            if mod.name == 'session':
                continue
            cache[mod.name] = mod.save()
        self.store[args.name] = cache

    def handle_load(self, args):
        if args.name not in self.store.keys():
            print(f'Session {args.name} not found')

        cache = self.store.get(args.name, {})
        for mod in self.client.modules:
            if mod.name == 'session':
                continue
            mod.load(cache.get(mod.name, {}))

    def handle_list(self, args):
        for name in self.store.keys():
            print(name)

    def handle_show(self, args):
        for mod in self.client.modules:
            cache = mod.save()
            if cache and mod.name not in {'session'}:
                print('{}:'.format(mod.name))
                print('  {}'.format(pprint.pformat(cache, indent=3)))
