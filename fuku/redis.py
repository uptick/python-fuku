from .module import Module


class Redis(Module):
    dependencies = ['task']

    def __init__(self, **kwargs):
        super().__init__('redis', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='redis help')

        p = subp.add_parser('mk', help='make a redis task')
        p.set_defaults(redis_handler=self.handle_make)

        p = subp.add_parser('connect')
        p.add_argument('target', metavar='TARGET', help='target task name')
        p.set_defaults(redis_handler=self.handle_connect)

    def handle_make(self, args):
        self.make()

    def make(self):
        task_mod = self.client.get_module('task')
        task_mod.make('redis', '!redis:alpine', memory=64)

    def handle_connect(self, args):
        self.connect(args.target)

    def connect(self, target):
        task_mod = self.client.get_module('task')
        env = {
            'REDIS_URL': self.get_url()
        }
        task_mod.env_set(target, env)

    def get_url(self):
        ctx = self.get_context()
        return f'redis://{ctx["app"]}-redis:6379'
