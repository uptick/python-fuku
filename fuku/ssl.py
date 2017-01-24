from .module import Module
from .utils import dict_to_env, dict_to_ports


class SSL(Module):
    dependencies = ['task']

    def __init__(self, **kwargs):
        super().__init__('ssl', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='ssl help')

        p = subp.add_parser('add')
        p.add_argument('task')
        p.add_argument('email')
        p.add_argument('domain')
        p.add_argument('upstream')
        p.add_argument('--staging', '-s', action='store_true')
        p.add_argument('--update', '-u', action='store_true')
        p.set_defaults(ssl_handler=self.handle_add)

    def handle_add(self, args):
        task_mod = self.client.get_module('task')
        if not args.update:
            task_mod.add(args.task, 'ssl', '/smashwilson/lets-nginx')
        env = {
            'EMAIL': args.email,
            'DOMAIN': args.domain,
            'UPSTREAM': '${dollar}' + args.upstream  # prefix for substitution
        }
        if args.staging:
            env['STAGING'] = '1'
        else:
            task_mod.env_unset(args.task, 'ssl', ['STAGING'])
        task_mod.env_set(args.task, 'ssl', env)
        ports = {
            '80': '80',
            '443': '443'
        }
        task_mod.ports_set(args.task, 'ssl', ports)
        ctr = args.upstream.split(':')[0]
        task_mod.link(args.task, 'ssl', ctr)
        # TODO: Remove reverse link.
