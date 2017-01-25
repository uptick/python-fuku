from .module import Module


class Papertrail(Module):
    dependencies = ['task']

    def __init__(self, **kwargs):
        super().__init__('papertrail', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='papertrail help')

        p = subp.add_parser('add')
        p.set_defaults(papertrail_handler=self.handle_add)

        p = subp.add_parser('connect')
        p.add_argument('target')
        p.set_defaults(papertrail_handler=self.handle_connect)

    def handle_add(self, args):
        task_mod = self.client.get_module('task')
        task_mod.add('papertrail', '!gliderlabs/logspout:latest',
                     mode='global')
        task_mod.mount_set('/var/run/docker.sock', '/var/run/docker.sock',
                           type='bind')
        task_mod.command('syslog+tls://{}'.format(args.log))
