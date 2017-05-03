from .module import Module


class Papertrail(Module):
    dependencies = ['task']

    def __init__(self, **kwargs):
        super().__init__('papertrail', **kwargs)

    def add_arguments(self, parser):
        subp = parser.add_subparsers(help='papertrail help')

        p = subp.add_parser('mk', help='make a papertrail task')
        p.add_argument('dest', metavar='DESTINATION', help='log destination')
        p.set_defaults(papertrail_handler=self.handle_make)

    def handle_make(self, args):
        self.make(args.dest)

    def make(self, dest):
        task_mod = self.client.get_module('task')
        task_mod.make('papertrail', '!gliderlabs/logspout:latest', logs=False)
        task_mod.volume_add('papertrail', 'socket', '/var/run/docker.sock', '/var/run/docker.sock', read_only=True)
        task_mod.command(f'syslog://{dest}')
