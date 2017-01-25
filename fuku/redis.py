from .module import Module
# from .utils import dict_to_env, dict_to_ports


class Redis(Module):
    dependencies = ['task']

    def __init__(self, **kwargs):
        super().__init__('redis', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='redis help')

        p = subp.add_parser('add')
        p.set_defaults(redis_handler=self.handle_add)

        p = subp.add_parser('connect')
        p.add_argument('target')
        p.set_defaults(redis_handler=self.handle_connect)

    def handle_add(self, args):
        task_mod = self.client.get_module('task')
        task_mod.add('redis', '!redis:alpine')

    def handle_connect(self, args):
        task_mod = self.client.get_module('task')
        env = {
            'REDIS_URL': self.get_url()
        }
        task_mod.env_set(args.target, env)

    def get_url(self):
        return 'redis://redis:6379'
