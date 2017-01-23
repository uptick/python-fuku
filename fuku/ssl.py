from .module import Module
from .utils import dict_to_env, dict_to_ports


class SSL(Module):
    dependencies = ['container']

    def __init__(self, **kwargs):
        super().__init__('ssl', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='ssl help')

        p = subp.add_parser('configure')
        p.add_argument('email')
        p.add_argument('domain')
        p.add_argument('upstream')
        p.add_argument('--staging', '-s', action='store_true')
        p.set_defaults(ssl_handler=self.handle_configure)

        p = subp.add_parser('run')
        p.add_argument('--restart', '-r', action='store_true')
        p.set_defaults(ssl_handler=self.handle_run)

    def handle_configure(self, args):
        app = self.client.get_selected('app')
        task_mod = self.client.get_module('task')
        name = '%s-letsnginx' % app
        env = {
            'EMAIL': args.email,
            'DOMAIN': args.domain,
            'UPSTREAM': args.upstream
        }
        if args.staging:
            env['STAGING'] = '1'
        ports = {
            '80': '80',
            '443': '443'
        }
        ctr_def = {
            'name': name,
            'image': 'smashwilson/lets-nginx',
            'memoryReservation': 1,
            'environment': dict_to_env(env),
            'portMappings': dict_to_ports(ports)
        }
        task_mod.register_task(ctr_def)

    def handle_run(self, args):
        ctr_mod = self.client.get_module('container')
        ctr_mod.run('letsnginx', 'letsnginx', args.restart)
